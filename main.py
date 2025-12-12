#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WeCafe Cleaning Bot (Telegram) — адаптирован под твою таблицу

Твои листы (как на скрине):
- cleaning_schedule  (план задач: task_id, task_name, point, photo_required, D1..D31)
- users             (пользователи бота)
- points            (список точек)
- shift_totals      (оставляем как есть — бот туда НЕ пишет)
- config / README   (бот не трогает)

Бот создаёт только 2 новых листа, если их нет:
- done_log          (что отметили выполненным)
- shift_log         (открытие смены)

ВАЖНО:
- Google JSON-ключ НЕ лежит рядом с кодом. Он берётся из переменной окружения.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from aiohttp import web

import pytz
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

# -------------------- ENV --------------------

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()

# Лучше: путь к json
GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "").strip()
# На крайний случай: base64 от json
GOOGLE_SHEETS_CREDENTIALS_JSON_B64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON_B64", "").strip()

TIME_ZONE = os.getenv("TIME_ZONE", "Europe/Moscow").strip()

ENABLE_HEALTH = os.getenv("ENABLE_HEALTH", "1").strip() != "0"
HEALTH_HOST = os.getenv("HEALTH_HOST", "127.0.0.1").strip()
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080").strip() or "8080")
CONTROL_GROUP_ID = int(os.getenv("CONTROL_GROUP_ID", "0").strip() or "0")
REPORT_TO_CONTROL = os.getenv("REPORT_TO_CONTROL", "1").strip() != "0"

# Webhook (Render)
WEBHOOK_MODE = os.getenv("WEBHOOK_MODE", "0").strip() == "1"
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip().rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "webhook").strip().lstrip("/")

# Напоминания по уборке
ENABLE_REMINDERS = os.getenv("ENABLE_REMINDERS", "1").strip() != "0"
REMINDER_INTERVAL_MINUTES = int(os.getenv("REMINDER_INTERVAL_MINUTES", "30").strip() or "30")

# Имена листов как на твоём файле
SHEET_SCHEDULE = os.getenv("SHEET_SCHEDULE", "cleaning_schedule").strip()
SHEET_USERS = os.getenv("SHEET_USERS", "users").strip()
SHEET_POINTS = os.getenv("SHEET_POINTS", "points").strip()
SHEET_DONE = os.getenv("SHEET_DONE", "done_log").strip()
SHEET_SHIFT = os.getenv("SHEET_SHIFT", "shift_log").strip()

# Заголовки (если лист пустой, бот добавит)
USERS_HEADER = ["user_id", "name", "point", "status", "created_at", "updated_at"]
DONE_HEADER = ["timestamp", "day", "user_id", "point", "task_id", "task_name", "photo_required", "photo_file_id"]
SHIFT_HEADER = ["timestamp", "day", "user_id", "point", "action"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Conversation states
GET_NAME, GET_POINT = range(2)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("wecafe-bot")

# -------------------- HELPERS --------------------

def now_tz() -> datetime:
    return datetime.now(pytz.timezone(TIME_ZONE))


def day_key() -> str:
    return now_tz().date().isoformat()  # YYYY-MM-DD


def day_column_name() -> str:
    # В твоей таблице колонки называются D1, D2, ... D31
    return f"D{now_tz().day}"


def sanitize_for_sheets(text: str) -> str:
    # защита от формул (= + - @)
    if not isinstance(text, str):
        text = str(text)
    text = text.strip()
    if text and text[0] in ("=", "+", "-", "@"):
        return "'" + text
    return text


def normalize_name(name: str) -> str:
    name = (name or "").strip()
    name = " ".join(name.split())
    name = name[:32]
    return sanitize_for_sheets(name)


def require_env():
    problems = []
    if not BOT_TOKEN:
        problems.append("BOT_TOKEN пустой")
    if not SPREADSHEET_ID:
        problems.append("SPREADSHEET_ID пустой")
    if not (GOOGLE_SHEETS_CREDENTIALS_FILE or GOOGLE_SHEETS_CREDENTIALS_JSON_B64):
        problems.append("нужен GOOGLE_SHEETS_CREDENTIALS_FILE или GOOGLE_SHEETS_CREDENTIALS_JSON_B64")
    if problems:
        raise RuntimeError("Проблемы с настройкой ENV: " + "; ".join(problems))


def format_control(title: str, user_name: str, user_id: int, point: str = "", details: Optional[List[str]] = None) -> str:
    lines = [title, f"Сотрудник: {user_name} ({user_id})"]
    if point:
        lines.append(f"Точка: {point}")
    if details:
        lines.extend(details)
    return "\n".join(lines)


async def report_to_control(
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    photo_file_id: Optional[str] = None,
    caption: str = "",
):
    """Отправляет сообщение (и опционально фото) в группу контроля. Ошибки глотаем, чтобы не ломать бизнес-логику."""
    if not REPORT_TO_CONTROL or CONTROL_GROUP_ID == 0:
        return
    try:
        await context.bot.send_message(chat_id=CONTROL_GROUP_ID, text=text)
    except Exception as e:
        log.warning("Не смог отправить сообщение в контроль: %s", e)

    if photo_file_id:
        try:
            await context.bot.send_photo(chat_id=CONTROL_GROUP_ID, photo=photo_file_id, caption=caption)
        except Exception as e:
            log.warning("Не смог отправить фото в контроль: %s", e)


# -------------------- GOOGLE SHEETS API --------------------

_svc = None


def _load_creds():
    if GOOGLE_SHEETS_CREDENTIALS_JSON_B64:
        raw = base64.b64decode(GOOGLE_SHEETS_CREDENTIALS_JSON_B64.encode("utf-8")).decode("utf-8")
        info = json.loads(raw)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return service_account.Credentials.from_service_account_file(GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=SCOPES)


def sheets_service():
    global _svc
    if _svc is None:
        _svc = build("sheets", "v4", credentials=_load_creds(), cache_discovery=False)
    return _svc


def sheet_get(range_a1: str) -> List[List[str]]:
    service = sheets_service()
    res = service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=range_a1).execute()
    return res.get("values", [])


def sheet_append(sheet_name: str, row: List[str]):
    service = sheets_service()
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=sheet_name,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()


def sheet_update(range_a1: str, row: List[str]):
    service = sheets_service()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=range_a1,
        valueInputOption="RAW",
        body={"values": [row]},
    ).execute()


def get_sheet_titles() -> List[str]:
    service = sheets_service()
    meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(title))",
    ).execute()
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def ensure_sheet_exists(sheet_title: str):
    titles = set(get_sheet_titles())
    if sheet_title in titles:
        return
    service = sheets_service()
    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": sheet_title}}}]},
    ).execute()


def ensure_header(sheet_title: str, header: List[str]):
    values = sheet_get(sheet_title)
    if not values:
        sheet_append(sheet_title, header)


def is_header(row: List[str], must_include: str) -> bool:
    low = [c.strip().lower() for c in row]
    return must_include.lower() in low


# -------------------- DATA: POINTS --------------------

def load_points() -> List[str]:
    rows = sheet_get(SHEET_POINTS)
    if not rows:
        # fallback
        return ["69 Параллель", "Арена", "Кафе Музей"]

    # берём первую колонку
    start = 1 if is_header(rows[0], "point") else 0
    pts = []
    for r in rows[start:]:
        if r and r[0].strip():
            pts.append(r[0].strip())
    return pts or ["69 Параллель", "Арена", "Кафе Музей"]


# -------------------- DATA: USERS --------------------

def get_user_row_and_index(user_id: int) -> Tuple[Optional[List[str]], Optional[int], bool]:
    rows = sheet_get(SHEET_USERS)
    if not rows:
        return None, None, False

    has_header = is_header(rows[0], "user_id")
    start = 1 if has_header else 0

    for i, row in enumerate(rows[start:], start=1 + start):
        if len(row) >= 1 and row[0] == str(user_id):
            return row, i, has_header
        # поддержка старых форматов
        if len(row) >= 2 and row[1] == str(user_id):
            return row, i, has_header

    return None, None, has_header


def is_user_active(user_id: int) -> bool:
    row, _, _ = get_user_row_and_index(user_id)
    if not row:
        return False
    # новый формат: статус в D
    if len(row) >= 4 and row[0] == str(user_id):
        return row[3] == "Активен"
    # старый формат
    if len(row) >= 4:
        return row[3] == "Активен"
    return False


def get_user_point(user_id: int) -> Optional[str]:
    row, _, _ = get_user_row_and_index(user_id)
    if not row:
        return None
    # новый формат: point в C
    if len(row) >= 3 and row[0] == str(user_id):
        return row[2]
    return None


def upsert_user(user_id: int, name: str, point: str, status: str = "Активен"):
    name = normalize_name(name)
    point = sanitize_for_sheets(point)
    ts = now_tz().isoformat(timespec="seconds")

    row, idx, _ = get_user_row_and_index(user_id)
    if row is None:
        sheet_append(SHEET_USERS, [str(user_id), name, point, status, ts, ts])
        return

    created_at = row[4] if len(row) >= 5 else ts
    new_row = [str(user_id), name, point, status, created_at, ts]
    sheet_update(f"{SHEET_USERS}!A{idx}:F{idx}", new_row)


def set_user_point(user_id: int, point: str):
    row, idx, _ = get_user_row_and_index(user_id)
    if row is None or idx is None:
        return
    ts = now_tz().isoformat(timespec="seconds")
    name = row[1] if len(row) >= 2 else ""
    status = row[3] if len(row) >= 4 else "Активен"
    created_at = row[4] if len(row) >= 5 else ts
    new_row = [str(user_id), name, sanitize_for_sheets(point), status, created_at, ts]
    sheet_update(f"{SHEET_USERS}!A{idx}:F{idx}", new_row)


# -------------------- DATA: SCHEDULE --------------------

@dataclass
class Task:
    task_id: str
    task_name: str
    point: str
    photo_required: bool


def _truthy(x: str) -> bool:
    s = (x or "").strip().lower()
    return s in ("1", "true", "yes", "да", "y", "ok")


def load_tasks_for_today(point_selected: str) -> List[Task]:
    """
    Берём из cleaning_schedule задачи, у которых:
    - в колонке D{сегодняшний день} стоит 1/TRUE
    - point == выбранная точка ИЛИ point == ALL
    """
    rows = sheet_get(SHEET_SCHEDULE)
    if not rows:
        return []

    header = rows[0]
    col = day_column_name()
    try:
        day_idx = header.index(col)
    except ValueError:
        return []

    tasks: List[Task] = []
    for r in rows[1:]:
        # ожидаем минимум: task_id, task_name, point, photo_required, + day column
        if len(r) <= max(3, day_idx):
            continue
        task_id = r[0].strip() if len(r) > 0 else ""
        task_name = r[1].strip() if len(r) > 1 else ""
        p = r[2].strip() if len(r) > 2 else ""
        photo_req = _truthy(r[3]) if len(r) > 3 else False
        flag = r[day_idx] if len(r) > day_idx else "0"

        if not task_id or not task_name:
            continue
        if not _truthy(flag):
            continue

        if p == "ALL" or p == point_selected:
            tasks.append(Task(task_id=task_id, task_name=task_name, point=p, photo_required=photo_req))

    return tasks


# -------------------- LOGS --------------------

def ensure_logs():
    # создаём листы логов (не трогаем твои shift_totals)
    ensure_sheet_exists(SHEET_DONE)
    ensure_sheet_exists(SHEET_SHIFT)
    ensure_header(SHEET_USERS, USERS_HEADER)
    ensure_header(SHEET_DONE, DONE_HEADER)
    ensure_header(SHEET_SHIFT, SHIFT_HEADER)


def log_done(user_id: int, point: str, task: Task, photo_file_id: str = ""):
    ts = now_tz().isoformat(timespec="seconds")
    sheet_append(
        SHEET_DONE,
        [
            ts,
            day_key(),
            str(user_id),
            sanitize_for_sheets(point),
            sanitize_for_sheets(task.task_id),
            sanitize_for_sheets(task.task_name),
            "TRUE" if task.photo_required else "FALSE",
            photo_file_id,
        ],
    )


def log_shift(user_id: int, point: str, action: str):
    ts = now_tz().isoformat(timespec="seconds")
    sheet_append(SHEET_SHIFT, [ts, day_key(), str(user_id), sanitize_for_sheets(point), sanitize_for_sheets(action)])


def get_done_task_ids_for_today(point: str, user_id: int) -> set[str]:
    """Возвращает множества task_id, которые этот пользователь уже закрыл сегодня на точке."""
    try:
        rows = sheet_get(f"{SHEET_DONE}!A2:H")
    except Exception:
        return set()

    today = day_key()
    uid = str(user_id)
    result: set[str] = set()

    for r in rows:
        if len(r) < 6:
            continue
        day_val = r[1]
        uid_val = r[2]
        point_val = r[3]
        task_id_val = r[4]
        if day_val != today:
            continue
        if uid_val != uid:
            continue
        if point_val != point:
            continue
        if not task_id_val:
            continue
        result.add(task_id_val)

    return result


def get_last_shift_state(user_id: int) -> tuple[bool, str]:
    """Возвращает (has_open_shift, last_point) по последней записи в shift_log."""
    try:
        rows = sheet_get(f"{SHEET_SHIFT}!A2:E")
    except Exception:
        return False, ""
    last_point = ""
    last_action = ""
    uid = str(user_id)
    for r in rows:
        if len(r) < 5:
            continue
        if r[2] != uid:
            continue
        last_point = r[3]
        last_action = r[4]
    return (last_action == "OPEN_SHIFT"), last_point


# -------------------- REMINDERS --------------------

def load_active_users() -> List[Tuple[int, str, str]]:
    """Активные пользователи из листа users: (user_id, name, point)."""
    rows = sheet_get(SHEET_USERS)
    if not rows:
        return []
    start = 1 if is_header(rows[0], "user_id") else 0
    out: List[Tuple[int, str, str]] = []
    for r in rows[start:]:
        if len(r) < 4:
            continue
        uid_raw = (r[0] or "").strip()
        status = (r[3] or "").strip()
        if status != "Активен":
            continue
        try:
            uid = int(uid_raw)
        except Exception:
            continue
        name = (r[1] if len(r) > 1 else "") or ""
        point = (r[2] if len(r) > 2 else "") or ""
        out.append((uid, name, point))
    return out


def get_open_shifts_map() -> Dict[int, str]:
    """Карта открытых смен: user_id -> point (по последнему действию в shift_log)."""
    try:
        rows = sheet_get(f"{SHEET_SHIFT}!A2:E")
    except Exception:
        return {}
    state: Dict[int, str] = {}
    for r in rows:
        if len(r) < 5:
            continue
        try:
            uid = int(r[2])
        except Exception:
            continue
        point = r[3] if len(r) > 3 else ""
        action = r[4] if len(r) > 4 else ""
        if action == "OPEN_SHIFT":
            state[uid] = point
        elif action == "CLOSE_SHIFT":
            state.pop(uid, None)
    return state


def get_done_ids_map_for_today(today: str) -> Dict[Tuple[int, str], set[str]]:
    """Карта отмеченных задач за сегодня: (user_id, point) -> set(task_id)."""
    try:
        rows = sheet_get(f"{SHEET_DONE}!A2:H")
    except Exception:
        return {}
    out: Dict[Tuple[int, str], set[str]] = {}
    for r in rows:
        if len(r) < 6:
            continue
        day_val = r[1]
        if day_val != today:
            continue
        try:
            uid = int(r[2])
        except Exception:
            continue
        point = r[3]
        task_id = r[4]
        if not point or not task_id:
            continue
        out.setdefault((uid, point), set()).add(task_id)
    return out


async def reminders_job(context: ContextTypes.DEFAULT_TYPE):
    """Раз в N минут пинаем сотрудников с открытой сменой, если есть невыполненные задачи уборки."""
    if not ENABLE_REMINDERS:
        return

    today = day_key()
    active_users = load_active_users()
    if not active_users:
        return

    open_map = get_open_shifts_map()
    if not open_map:
        return

    done_map = get_done_ids_map_for_today(today)

    for uid, name, default_point in active_users:
        point = open_map.get(uid)
        if not point:
            continue  # смена не открыта

        tasks = load_tasks_for_today(point)
        if not tasks:
            continue

        done_ids = done_map.get((uid, point), set())
        remaining = [t for t in tasks if t.task_id not in done_ids]
        if not remaining:
            continue

        lines = [f"⏰ Напоминание: по уборке осталось задач: {len(remaining)}"]
        for t in remaining[:7]:
            photo_icon = " 📸" if t.photo_required else ""
            lines.append(f"• {t.task_name}{photo_icon}")
        if len(remaining) > 7:
            lines.append("…")
        lines.append("\nОткрой меню: /menu")
        try:
            await context.bot.send_message(chat_id=uid, text="\n".join(lines))
        except Exception as e:
            log.warning("Не смог отправить напоминание пользователю %s: %s", uid, e)


# -------------------- UI --------------------

def main_menu(user_id: int) -> InlineKeyboardMarkup:
    """Главное меню, зависящее от того, открыта ли смена."""
    has_open, last_point = get_last_shift_state(user_id)

    rows = []
    # Кнопка выбора точки: только когда смена ЗАКРЫТА
    if not has_open:
        rows.append([InlineKeyboardButton("📍 Выбор точки", callback_data="CHOOSE_POINT")])

    rows.append([InlineKeyboardButton("🧾 Посмотреть план уборки", callback_data="VIEW_PLAN")])
    rows.append([InlineKeyboardButton("✅ Отметить выполненное", callback_data="MARK_DONE")])
    rows.append([InlineKeyboardButton("📸 Отправить фото для отметки", callback_data="HELP_PHOTO")])

    # Кнопка смены: либо открыть, либо закрыть
    if has_open:
        rows.append([InlineKeyboardButton("🔒 Закрыть смену", callback_data="CLOSE_SHIFT")])
    else:
        rows.append([InlineKeyboardButton("🔓 Открыть смену", callback_data="OPEN_SHIFT")])

    return InlineKeyboardMarkup(rows)


def points_keyboard(points: List[str], prefix: str) -> InlineKeyboardMarkup:
    btns = [[InlineKeyboardButton(p, callback_data=f"{prefix}|{i}")] for i, p in enumerate(points)]
    btns.append([InlineKeyboardButton("⬅️ Назад", callback_data="BACK_MENU")])
    return InlineKeyboardMarkup(btns)


def tasks_keyboard(tasks: List[Task]) -> InlineKeyboardMarkup:
    btns = []
    for i, t in enumerate(tasks):
        icon = "📸 " if t.photo_required else ""
        label = f"{icon}{t.task_name}"
        if len(label) > 48:
            label = label[:45] + "…"
        btns.append([InlineKeyboardButton(f"✅ {label}", callback_data=f"DONE|{i}")])
    btns.append([InlineKeyboardButton("⬅️ Назад", callback_data="BACK_MENU")])
    return InlineKeyboardMarkup(btns)


# -------------------- HANDLERS --------------------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if is_user_active(user_id):
        p = get_user_point(user_id)
        if p:
            context.user_data["point"] = p
        await update.message.reply_text("Привет! Я тебя узнал 🙂\nМеню:", reply_markup=main_menu(user_id))
        point = current_point(context, user_id)
        await report_to_control(
            context,
            format_control("▶️ /start (активный пользователь)", update.effective_user.full_name, user_id, point=point),
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "Привет! Давай зарегистрируемся.\n\nНапиши своё имя:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return GET_NAME


async def handle_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = normalize_name(update.message.text)
    if len(name) < 2:
        await update.message.reply_text("Имя слишком короткое. Напиши хотя бы 2 буквы.")
        return GET_NAME
    context.user_data["reg_name"] = name

    pts = load_points()
    context.user_data["points_list"] = pts
    await update.message.reply_text("Теперь выбери точку:", reply_markup=points_keyboard(pts, prefix="REGPOINT"))
    return GET_POINT


async def handle_reg_point(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    pts = context.user_data.get("points_list") or load_points()
    try:
        _, idx_s = q.data.split("|", 1)
        idx = int(idx_s)
        point = pts[idx]
    except Exception:
        await q.edit_message_text("Не понял выбор. Давай ещё раз:", reply_markup=points_keyboard(pts, prefix="REGPOINT"))
        return GET_POINT

    user_id = q.from_user.id
    name = context.user_data.get("reg_name", "Без имени")
    upsert_user(user_id, name, point, status="Активен")
    context.user_data["point"] = point

    await q.edit_message_text(f"Готово! Ты зарегистрирован.\nТочка: {point}")
    await q.message.reply_text("Меню:", reply_markup=main_menu(user_id))

    await report_to_control(
        context,
        format_control("✅ Регистрация сотрудника", q.from_user.full_name, user_id, point=point),
    )
    return ConversationHandler.END


async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_active(user_id):
        await update.message.reply_text("Сначала регистрация: /start")
        return
    await update.message.reply_text("Меню:", reply_markup=main_menu(user_id))


async def back_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    await q.edit_message_text("Меню:", reply_markup=main_menu(user_id))

    point = current_point(context, user_id)
    await report_to_control(
        context,
        format_control("↩️ Возврат в меню", q.from_user.full_name, user_id, point=point),
    )


def current_point(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> str:
    p = context.user_data.get("point")
    if p:
        return p
    p = get_user_point(user_id)
    if p:
        context.user_data["point"] = p
        return p
    pts = load_points()
    return pts[0] if pts else "ALL"


async def choose_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pts = load_points()
    context.user_data["points_list"] = pts
    await q.edit_message_text("Выбери точку:", reply_markup=points_keyboard(
