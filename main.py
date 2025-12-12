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

async def report_to_control(context: ContextTypes.DEFAULT_TYPE, text: str, photo_file_id: Optional[str] = None, caption: str = ""):
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
        fields="sheets(properties(title))"
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
        # если вдруг колонка не найдена
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
    sheet_append(SHEET_DONE, [
        ts, day_key(), str(user_id), sanitize_for_sheets(point),
        sanitize_for_sheets(task.task_id), sanitize_for_sheets(task.task_name),
        "TRUE" if task.photo_required else "FALSE",
        photo_file_id
    ])

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
        # day, user_id, point, task_id
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

        # Сообщение сотруднику
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

    await update.message.reply_text("Привет! Давай зарегистрируемся.\n\nНапиши своё имя:", reply_markup=ReplyKeyboardRemove())
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

    # Лог в контроль
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
    # если вообще нет — первая точка из points
    pts = load_points()
    return pts[0] if pts else "ALL"

async def choose_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pts = load_points()
    context.user_data["points_list"] = pts
    await q.edit_message_text("Выбери точку:", reply_markup=points_keyboard(pts, prefix="POINT"))

async def set_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    pts = context.user_data.get("points_list") or load_points()
    try:
        _, idx_s = q.data.split("|", 1)
        idx = int(idx_s)
        point = pts[idx]
    except Exception:
        await q.edit_message_text("Не понял выбор. Давай ещё раз:", reply_markup=points_keyboard(pts, prefix="POINT"))
        return

    user_id = q.from_user.id
    set_user_point(user_id, point)
    context.user_data["point"] = point
    await q.edit_message_text(f"Ок! Точка теперь: {point}", reply_markup=main_menu(user_id))

    await report_to_control(
        context,
        format_control("📍 Смена точки", q.from_user.full_name, user_id, point=point),
    )

async def view_plan_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    point = current_point(context, user_id)

    tasks = load_tasks_for_today(point)
    col = day_column_name()

    if not tasks:
        await q.edit_message_text(
            f"На сегодня задач нет 🙂\n(колонка {col})",
            reply_markup=main_menu(user_id),
        )
        return

    # Какие задачи уже отмечены этим пользователем на этой точке
    done_ids = get_done_task_ids_for_today(point, user_id)

    lines: list[str] = []
    for t in tasks:
        status = "✅" if t.task_id in done_ids else "⬜"
        photo_icon = " 📸" if t.photo_required else ""
        lines.append(f"{status} {t.task_name}{photo_icon}")

    text = f"План на сегодня ({day_key()}, колонка {col}):\n" + "\n".join(lines)
    await q.edit_message_text(text, reply_markup=main_menu(user_id))

    await report_to_control(
        context,
        format_control("🧾 Просмотр плана уборки", q.from_user.full_name, user_id, point=point, details=[f"Колонка: {col}"]),
    )



async def mark_done_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    point = current_point(context, user_id)
    tasks = load_tasks_for_today(point)

    if not tasks:
        await q.edit_message_text(
            "Сегодня нечего отмечать 🙂",
            reply_markup=main_menu(user_id),
        )
        return

    # Фильтруем уже отмеченные задачи, чтобы нельзя было спамить
    done_ids = get_done_task_ids_for_today(point, user_id)
    remaining = [t for t in tasks if t.task_id not in done_ids]

    if not remaining:
        await q.edit_message_text(
            "Все задачи на сегодня уже отмечены ✅",
            reply_markup=main_menu(user_id),
        )
        return

    context.user_data["today_tasks"] = remaining
    await q.edit_message_text(
        "Что выполнено? Нажми на задачу:",
        reply_markup=tasks_keyboard(remaining),
    )


async def done_pick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    point = current_point(context, user_id)

    tasks: List[Task] = context.user_data.get("today_tasks", [])
    try:
        _, idx_s = q.data.split("|", 1)
        idx = int(idx_s)
        task = tasks[idx]
    except Exception:
        await q.edit_message_text("Я запутался 😅 Нажми «Отметить выполненное» ещё раз.", reply_markup=main_menu(user_id))
        return

    # Если нужно фото — попросим фото
    if task.photo_required:
        # запоминаем, что ждём фото именно для задачи
        context.user_data["await_photo_mode"] = "TASK"
        context.user_data["await_photo_task"] = {
            "task_id": task.task_id,
            "task_name": task.task_name,
            "photo_required": True,
        }
        await q.edit_message_text(
            "Эта задача требует фото 📸\n\n"
            "Сейчас просто отправь мне ОДНО фото сообщением.\n"
            "После фото я сам запишу отметку.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Отмена", callback_data="CANCEL_PHOTO")]])
        )
        return

    # Без фото — сразу логируем
    log_done(user_id, point, task, photo_file_id="")
    await q.edit_message_text(f"Записал ✅\n{task.task_name}", reply_markup=main_menu(user_id))

    await report_to_control(
        context,
        format_control("✅ Уборка выполнена (без фото)", q.from_user.full_name, user_id, point=point, details=[f"Задача: {task.task_name}"]),
    )

async def cancel_photo_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data.pop("await_photo_task", None)
    context.user_data.pop("await_photo_mode", None)
    await q.edit_message_text("Ок, отменил. Меню:", reply_markup=main_menu(user_id))

async def photo_help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    point = current_point(context, user_id)
    await q.edit_message_text(
        "Как отправить фото:\n"
        "1) Нажми «Отметить выполненное»\n"
        "2) Выбери задачу с значком 📸\n"
        "3) Потом отправь фото обычным сообщением\n\n"
        "Я сам всё запишу в таблицу ✅",
        reply_markup=main_menu(user_id)
    )

    await report_to_control(
        context,
        format_control("ℹ️ Открыта справка по фото", q.from_user.full_name, user_id, point=point),
    )


async def photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка входящего фото.

    Варианты:
    - await_photo_mode == "TASK"  и есть await_photo_task -> фото к задаче уборки
    - await_photo_mode == "CLOSE_SHIFT1" / "CLOSE_SHIFT2" -> фото чеков при закрытии смены
    - иначе вежливо подсказываем, что нужно нажать в меню
    """
    user_id = update.effective_user.id
    mode = context.user_data.get("await_photo_mode")
    payload = context.user_data.get("await_photo_task")

    # берём самое большое фото
    if not update.message.photo:
        return
    photo = update.message.photo[-1]
    file_id = photo.file_id

    # 1) Фото для закрытия смены — первый чек
    if mode == "CLOSE_SHIFT1":
        closing = context.user_data.get("closing_shift") or {}
        point = closing.get("point", current_point(context, user_id))

        if REPORT_TO_CONTROL and CONTROL_GROUP_ID != 0:
            text = (
                "🧾 Чек 1 (открытие смены)\n"
                f"Точка: {point}\n"
                f"Сотрудник: {user_id}"
            )
            try:
                await context.bot.send_message(chat_id=CONTROL_GROUP_ID, text=text)
            except Exception:
                pass
            try:
                await context.bot.send_photo(
                    chat_id=CONTROL_GROUP_ID,
                    photo=file_id,
                    caption=f"Чек 1 — точка: {point}",
                )
            except Exception:
                pass

        # ждём второй чек
        context.user_data["await_photo_mode"] = "CLOSE_SHIFT2"
        await update.message.reply_text(
            "Принял первый чек ✅\nТеперь пришли фото ЧЕКА ЗАКРЫТИЯ смены.",
            reply_markup=main_menu(user_id),
        )
        return

    # 2) Фото для закрытия смены — второй чек, после него реально закрываем смену
    if mode == "CLOSE_SHIFT2":
        closing = context.user_data.get("closing_shift") or {}
        point = closing.get("point", current_point(context, user_id))
        missing = closing.get("missing_names", [])

        if REPORT_TO_CONTROL and CONTROL_GROUP_ID != 0:
            text = (
                "🧾 Чек 2 (закрытие смены)\n"
                f"Точка: {point}\n"
                f"Сотрудник: {user_id}"
            )
            try:
                await context.bot.send_message(chat_id=CONTROL_GROUP_ID, text=text)
            except Exception:
                pass
            try:
                await context.bot.send_photo(
                    chat_id=CONTROL_GROUP_ID,
                    photo=file_id,
                    caption=f"Чек 2 — точка: {point}",
                )
            except Exception:
                pass

        # Логируем закрытие смены
        log_shift(user_id, point, "CLOSE_SHIFT")

        # Формируем текст про косяк / успех по уборке
        base = f"Смена закрыта ✅\nТочка: {point}"
        if missing:
            base += "\n\n⚠️ План уборки выполнен не полностью. Это косяк 😈\nНе отмечены задачи:"
            for name in missing:
                base += f"\n• {name}"
        else:
            base += "\n\nПлан уборки выполнен полностью 💪"

        # Отправляем итог в контроль (косяк/успех)
        details: List[str] = []
        if missing:
            details.append("⚠️ Косяк: план уборки НЕ полностью")
            for n in missing[:15]:
                details.append(f"• {n}")
            if len(missing) > 15:
                details.append("…")
        else:
            details.append("✅ План уборки полностью")
        await report_to_control(
            context,
            format_control("🔒 Закрытие смены", update.effective_user.full_name, user_id, point=point, details=details),
        )

        # Чистим состояние
        context.user_data.pop("closing_shift", None)
        context.user_data.pop("await_photo_mode", None)

        await update.message.reply_text(base, reply_markup=main_menu(user_id))
        return

    # 3) Фото для задачи уборки
    if mode == "TASK" and payload:
        point = current_point(context, user_id)
        task = Task(
            task_id=payload["task_id"],
            task_name=payload["task_name"],
            point=point,
            photo_required=True,
        )
        log_done(user_id, point, task, photo_file_id=file_id)

        if REPORT_TO_CONTROL and CONTROL_GROUP_ID != 0:
            text = (
                "📸 Уборка выполнена (с фото)\n"
                f"Точка: {point}\n"
                f"Сотрудник: {user_id}\n"
                f"Задача: {task.task_name}"
            )
            try:
                await context.bot.send_message(chat_id=CONTROL_GROUP_ID, text=text)
            except Exception:
                pass
            try:
                await context.bot.send_photo(
                    chat_id=CONTROL_GROUP_ID,
                    photo=file_id,
                    caption=f"Точка: {point}\nЗадача: {task.task_name}",
                )
            except Exception:
                pass

        context.user_data.pop("await_photo_task", None)
        context.user_data.pop("await_photo_mode", None)

        await update.message.reply_text(
            f"Готово ✅ Фото записал и отметил задачу:\n{task.task_name}",
            reply_markup=main_menu(user_id),
        )
        return

    # 4) Бот фото не ждёт — подсказываем, что делать
    await update.message.reply_text(
        "Фото получил 👍\n"
        "Но сейчас я ни с какой задачей и сменой фото не жду.\n"
        "Нажми «Отметить выполненное» или кнопку закрытия смены в меню.",
        reply_markup=main_menu(user_id),
    )

    await report_to_control(
        context,
        format_control("📷 Фото отправлено вне сценария", update.effective_user.full_name, user_id, point=current_point(context, user_id)),
        photo_file_id=file_id,
        caption=f"Пользователь прислал фото вне сценария. user_id={user_id}",
    )


async def open_shift_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    point = current_point(context, user_id)
    log_shift(user_id, point, "OPEN_SHIFT")
    await q.edit_message_text(f"Смена открыта ✅\nТочка: {point}", reply_markup=main_menu(user_id))

    await report_to_control(
        context,
        format_control("🔓 Открытие смены", q.from_user.full_name, user_id, point=point),
    )



async def close_shift_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Закрытие смены с двумя фото чеков и проверкой плана уборки."""
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    has_open, point = get_last_shift_state(user_id)
    if not has_open or not point:
        await q.edit_message_text(
            "У тебя сейчас нет открытой смены 🤔",
            reply_markup=main_menu(user_id),
        )
        return

    # Считаем, какие задачи по плану уборки ещё не отмечены
    tasks = load_tasks_for_today(point)
    done_ids = get_done_task_ids_for_today(point, user_id)
    missing = [t.task_name for t in tasks if t.task_id not in done_ids]

    # Запомним данные о закрытии смены и попросим 2 фото чеков
    context.user_data["closing_shift"] = {
        "point": point,
        "missing_names": missing,
    }
    context.user_data["await_photo_mode"] = "CLOSE_SHIFT1"

    await q.edit_message_text(
        "Перед закрытием смены пришли, пожалуйста, ДВА фото чеков:\n"
        "1️⃣ Фото чека ОТКРЫТИЯ смены\n"
        "2️⃣ Фото чека ЗАКРЫТИЯ смены\n\n"
        "Сначала отправь первый чек фото сообщением.",
        reply_markup=main_menu(user_id),
    )

    await report_to_control(
        context,
        format_control(
            "🔒 Начато закрытие смены (ждём 2 фото чеков)",
            q.from_user.full_name,
            user_id,
            point=point,
            details=[f"Невыполненных задач: {len(missing)}"],
        ),
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Ошибка: %s", context.error)

# -------------------- HEALTH --------------------

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health", "/healthz"):
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"OK")

def start_health_server():
    if not ENABLE_HEALTH:
        return
    def _run():
        srv = HTTPServer((HEALTH_HOST, HEALTH_PORT), HealthHandler)
        log.info("Health: http://%s:%s/healthz", HEALTH_HOST, HEALTH_PORT)
        srv.serve_forever()
    threading.Thread(target=_run, daemon=True).start()

# -------------------- APP --------------------

def build_app() -> Application:
    require_env()
    # Проверка доступа и подготовка логов/заголовков
    try:
        ensure_logs()
    except HttpError as e:
        raise RuntimeError(
            "Не получилось подключиться к таблице.\n"
            "Проверь:\n"
            "1) SPREADSHEET_ID\n"
            "2) что сервис-аккаунт добавлен в «Поделиться» как Редактор\n"
            f"\nОшибка: {e}"
        ) from e

    app = Application.builder().token(BOT_TOKEN).build()

    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start_cmd)],
        states={
            GET_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_name)],
            GET_POINT: [CallbackQueryHandler(handle_reg_point, pattern=r"^REGPOINT\|\d+$")],
        },
        fallbacks=[CommandHandler("menu", menu_cmd)],
    )
    app.add_handler(reg_conv)

    app.add_handler(CommandHandler("menu", menu_cmd))

    app.add_handler(CallbackQueryHandler(back_menu_cb, pattern=r"^BACK_MENU$"))
    app.add_handler(CallbackQueryHandler(choose_point_cb, pattern=r"^CHOOSE_POINT$"))
    app.add_handler(CallbackQueryHandler(set_point_cb, pattern=r"^POINT\|\d+$"))

    app.add_handler(CallbackQueryHandler(view_plan_cb, pattern=r"^VIEW_PLAN$"))

    app.add_handler(CallbackQueryHandler(mark_done_cb, pattern=r"^MARK_DONE$"))
    app.add_handler(CallbackQueryHandler(done_pick_cb, pattern=r"^DONE\|\d+$"))
    app.add_handler(CallbackQueryHandler(cancel_photo_cb, pattern=r"^CANCEL_PHOTO$"))
    app.add_handler(CallbackQueryHandler(photo_help_cb, pattern=r"^HELP_PHOTO$"))
    app.add_handler(MessageHandler(filters.PHOTO, photo_message))

    app.add_handler(CallbackQueryHandler(open_shift_cb, pattern=r"^OPEN_SHIFT$"))
    app.add_handler(CallbackQueryHandler(close_shift_cb, pattern=r"^CLOSE_SHIFT$"))

    app.add_error_handler(error_handler)

    # Планировщик напоминаний
    if ENABLE_REMINDERS and app.job_queue:
        interval = max(5, REMINDER_INTERVAL_MINUTES) * 60
        app.job_queue.run_repeating(reminders_job, interval=interval, first=interval, name="cleaning_reminders")
        log.info("Reminders enabled: every %s minutes", REMINDER_INTERVAL_MINUTES)
    else:
        log.info("Reminders disabled or JobQueue not available")

    return app

def main():
    start_health_server()
    app = build_app()
    # --- START BOT (Polling or Webhook) ---
import os

WEBHOOK_MODE = os.getenv("WEBHOOK_MODE", "0") == "1"

if WEBHOOK_MODE:
    PORT = int(os.getenv("PORT", "10000"))
    BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").rstrip("/")
    PATH = os.getenv("WEBHOOK_PATH", "webhook").lstrip("/")

    if not BASE_URL:
        raise RuntimeError("WEBHOOK_BASE_URL is empty (set it in Render Environment)")

    log.info(f"Webhook mode ON: {BASE_URL}/{PATH}  port={PORT}")

    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=PATH,
        webhook_url=f"{BASE_URL}/{PATH}",
        drop_pending_updates=True,
    )
else:
    log.info("Polling mode ON")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
