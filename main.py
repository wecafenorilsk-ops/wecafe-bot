#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WeCafe Shift & Tasks Bot (Telegram) — версия под сценарий DreamTeam

Основано на текущем работающем боте и его подходе к Google Sheets (users/points/cleaning_schedule + лог-листы). fileciteturn0file0

Сценарий:
- регистрация: Имя -> код DreamTeam -> запрос на одобрение в группу контроля
- после одобрения: только выбор точки -> затем только 2 кнопки открытия смены (полная/пол смены)
- полная смена: фото готовности витрины при открытии; план задач на день; отметка задач с 1-2 фото; напоминания раз в час при бездействии; закрытие только в конце смены с цифрами + 2 фото чеков + 4 фото уборки
- пол смены: задачи делятся пополам; у 1-го есть кнопка передачи смены конкретному 2-му сотруднику на этой точке; закрытие смены (чеки/цифры/уборка) только у 2-го
- “Красавчик помоги”: сообщение + до 4 фото в контроль
- админ в группе контроля может блокировать/разблокировать сотрудника

Деплой:
- Render / Webhook или Polling (как в текущем коде)
- Google JSON ключ: GOOGLE_SHEETS_CREDENTIALS_FILE или GOOGLE_SHEETS_CREDENTIALS_JSON_B64 (base64)
"""

from __future__ import annotations

import base64
import json
import html
import logging
import os
import threading
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List, Optional, Tuple, Any

import pytz
from aiohttp import web
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ChatType
from telegram.error import BadRequest
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

GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON_B64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON_B64", "").strip()

# Часовой пояс по ТЗ: Красноярский край
TIME_ZONE = os.getenv("TIME_ZONE", "Asia/Krasnoyarsk").strip()

CONTROL_GROUP_ID = int(os.getenv("CONTROL_GROUP_ID", "0").strip() or "0")
REPORT_TO_CONTROL = os.getenv("REPORT_TO_CONTROL", "1").strip() != "0"

ACCESS_CODE = os.getenv("ACCESS_CODE", "DreamTeam").strip()

# Webhook (Render)
WEBHOOK_MODE = os.getenv("WEBHOOK_MODE", "0").strip() == "1"
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip().rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "webhook").strip().lstrip("/")
MAX_WEBHOOK_QUEUE = int(os.getenv("MAX_WEBHOOK_QUEUE", "1000").strip() or "1000")

# Health
ENABLE_HEALTH = os.getenv("ENABLE_HEALTH", "1").strip() != "0"
HEALTH_HOST = os.getenv("HEALTH_HOST", "127.0.0.1").strip()
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080").strip() or "8080")

# Напоминания
ENABLE_REMINDERS = os.getenv("ENABLE_REMINDERS", "1").strip() != "0"
REMINDER_CHECK_MINUTES = int(os.getenv("REMINDER_CHECK_MINUTES", "10").strip() or "10")  # проверяем чаще, пинаем раз в час
REMINDER_IDLE_MINUTES = int(os.getenv("REMINDER_IDLE_MINUTES", "60").strip() or "60")

# Ежедневные итоги (в группу контроля)
ENABLE_DAILY_TOTALS = os.getenv("ENABLE_DAILY_TOTALS", "1").strip() != "0"
DAILY_TOTALS_HOUR = int(os.getenv("DAILY_TOTALS_HOUR", "23").strip() or "23")
DAILY_TOTALS_MINUTE = int(os.getenv("DAILY_TOTALS_MINUTE", "50").strip() or "50")

# Листы (сохраняем “дух” текущего бота)
SHEET_SCHEDULE = os.getenv("SHEET_SCHEDULE", "cleaning_schedule").strip()
SHEET_USERS = os.getenv("SHEET_USERS", "users").strip()
SHEET_POINTS = os.getenv("SHEET_POINTS", "points").strip()

# Логи и служебные
SHEET_DONE = os.getenv("SHEET_DONE", "done_log").strip()                # отметки задач
SHEET_SESSIONS = os.getenv("SHEET_SESSIONS", "shift_sessions").strip()  # состояния смен
SHEET_CLOSE = os.getenv("SHEET_CLOSE", "close_log").strip()             # закрытие смены (цифры + фото)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("wecafe-shift-bot")


# -------------------- TELEGRAM HELPERS --------------------

async def safe_edit(q, *args, **kwargs):
    """edit_message_text without crashing on 'Message is not modified'."""
    try:
        return await q.edit_message_text(*args, **kwargs)
    except BadRequest as e:
        # Happens when user taps the same inline button twice or markup/text didn't change.
        if "Message is not modified" in str(e):
            return None
        raise

# -------------------- TIME HELPERS --------------------

_tz = pytz.timezone(TIME_ZONE)


def now_tz() -> datetime:
    return datetime.now(_tz)


def day_key() -> str:
    return now_tz().date().isoformat()  # YYYY-MM-DD


def day_column_name() -> str:
    # в cleaning_schedule: D1..D31
    return f"D{now_tz().day}"


# -------------------- SANITIZE --------------------


def sanitize_for_sheets(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    text = text.strip()
    if text and text[0] in ("=", "+", "-", "@"):
        return "'" + text
    return text


def normalize_name(name: str) -> str:
    name = (name or "").strip()
    name = " ".join(name.split())
    return sanitize_for_sheets(name[:32])


# -------------------- ENV CHECK --------------------


def require_env():
    problems = []
    if not BOT_TOKEN:
        problems.append("BOT_TOKEN пустой")
    if not SPREADSHEET_ID:
        problems.append("SPREADSHEET_ID пустой")
    if not (GOOGLE_SHEETS_CREDENTIALS_FILE or GOOGLE_SHEETS_CREDENTIALS_JSON_B64):
        problems.append("нужен GOOGLE_SHEETS_CREDENTIALS_FILE или GOOGLE_SHEETS_CREDENTIALS_JSON_B64")
    if CONTROL_GROUP_ID == 0:
        problems.append("CONTROL_GROUP_ID не задан (нужен для одобрения/отчетов)")
    if problems:
        raise RuntimeError("Проблемы с настройкой ENV: " + "; ".join(problems))


# -------------------- CONTROL GROUP REPORT --------------------


def format_control(title: str, user_name: str, user_id: int, point: str = "", details: Optional[List[str]] = None) -> str:
    lines = [title, f"Сотрудник: {user_name} ({user_id})"]
    if point:
        lines.append(f"Точка: {point}")
    if details:
        lines.extend(details)
    return "\n".join(lines)


async def report_to_control(context: ContextTypes.DEFAULT_TYPE, text: str):
    if not REPORT_TO_CONTROL or CONTROL_GROUP_ID == 0:
        return
    try:
        await context.bot.send_message(chat_id=CONTROL_GROUP_ID, text=text)
    except Exception as e:
        log.warning("Не смог отправить сообщение в контроль: %s", e)


async def report_photo_to_control(context: ContextTypes.DEFAULT_TYPE, file_id: str, caption: str = ""):
    if not REPORT_TO_CONTROL or CONTROL_GROUP_ID == 0:
        return
    try:
        await context.bot.send_photo(chat_id=CONTROL_GROUP_ID, photo=file_id, caption=caption)
    except Exception as e:
        log.warning("Не смог отправить фото в контроль: %s", e)


# -------------------- GOOGLE SHEETS --------------------

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


# -------------------- SCHEMAS --------------------
# users: сохраняем совместимость с текущим: user_id, name, point, status, created_at, updated_at
# status: "На одобрении" | "Активен" | "Заблокирован"
USERS_HEADER = ["user_id", "name", "point", "status", "created_at", "updated_at"]

# done_log: расширяем (не ломая): timestamp, day, point, user_id, user_name, task_id, task_name, part, photo1, photo2
DONE_HEADER = ["timestamp", "day", "point", "user_id", "user_name", "task_id", "task_name", "part", "photo1_file_id", "photo2_file_id"]

# shift_sessions: единая запись на точку/день
# session_id, day, point, mode(FULL|HALF), state, user1_id, user1_name, user1_start, user1_end,
# user2_id, user2_name, user2_start, user2_end, split_index, updated_at
SESSIONS_HEADER = [
    "session_id", "day", "point", "mode", "state",
    "user1_id", "user1_name", "user1_start", "user1_end",
    "user2_id", "user2_name", "user2_start", "user2_end",
    "split_index", "updated_at",
]

# close_log: фиксация закрытия смены
CLOSE_HEADER = [
    "timestamp", "day", "point", "session_id", "mode",
    "user_id", "user_name",
    "cash_in", "sales_cashless", "sales_cash", "refunds",
    "total_sales", "cash_in_box",
    "receipt1_file_id", "receipt2_file_id",
    "cleanup1_file_id", "cleanup2_file_id", "cleanup3_file_id", "cleanup4_file_id",
    "note",
]

# -------------------- BOOTSTRAP SHEETS --------------------


def ensure_sheets():
    ensure_sheet_exists(SHEET_USERS)
    ensure_sheet_exists(SHEET_POINTS)
    ensure_sheet_exists(SHEET_SCHEDULE)
    ensure_sheet_exists(SHEET_DONE)
    ensure_sheet_exists(SHEET_SESSIONS)
    ensure_sheet_exists(SHEET_CLOSE)

    ensure_header(SHEET_USERS, USERS_HEADER)
    ensure_header(SHEET_DONE, DONE_HEADER)
    ensure_header(SHEET_SESSIONS, SESSIONS_HEADER)
    ensure_header(SHEET_CLOSE, CLOSE_HEADER)


# -------------------- POINTS --------------------


DEFAULT_POINTS = ["69 Параллель", "Арена", "Музей", "Сочнева"]


def load_points() -> List[str]:
    rows = sheet_get(SHEET_POINTS)
    if not rows:
        return DEFAULT_POINTS
    start = 1 if is_header(rows[0], "point") else 0
    pts: List[str] = []
    for r in rows[start:]:
        if r and r[0].strip():
            pts.append(r[0].strip())
    return pts or DEFAULT_POINTS


def normalize_point(point: str) -> str:
    p = (point or "").strip()
    # мягкая нормализация под варианты из старой таблицы
    if "музей" in p.lower():
        return "Музей"
    if "сочнев" in p.lower():
        return "Сочнева"
    if "арена" in p.lower():
        return "Арена"
    if "69" in p or "паралл" in p.lower():
        return "69 Параллель"
    return p


# -------------------- USERS --------------------


@dataclass
class UserRec:
    user_id: int
    name: str
    point: str
    status: str
    created_at: str
    updated_at: str


STATUS_PENDING = "На одобрении"
STATUS_ACTIVE = "Активен"
STATUS_BLOCKED = "Заблокирован"


def _users_rows() -> Tuple[List[List[str]], bool]:
    rows = sheet_get(SHEET_USERS)
    if not rows:
        return [], False
    has_header = is_header(rows[0], "user_id")
    return rows, has_header


def get_user_row_and_index(user_id: int) -> Tuple[Optional[List[str]], Optional[int], bool]:
    rows, has_header = _users_rows()
    if not rows:
        return None, None, has_header
    start = 1 if has_header else 0
    for i, row in enumerate(rows[start:], start=1 + start):
        if len(row) >= 1 and row[0] == str(user_id):
            return row, i, has_header
    return None, None, has_header


def parse_user(row: List[str]) -> UserRec:
    # ожидаем порядок как USERS_HEADER
    uid = int(row[0])
    name = row[1] if len(row) > 1 else ""
    point = row[2] if len(row) > 2 else ""
    status = row[3] if len(row) > 3 else STATUS_PENDING
    created_at = row[4] if len(row) > 4 else ""
    updated_at = row[5] if len(row) > 5 else ""
    return UserRec(uid, name, point, status, created_at, updated_at)


def get_user(user_id: int) -> Optional[UserRec]:
    row, _, _ = get_user_row_and_index(user_id)
    if not row:
        return None
    try:
        return parse_user(row)
    except Exception:
        return None


def upsert_user(user_id: int, name: str, point: str = "", status: str = STATUS_PENDING):
    name = normalize_name(name)
    point = sanitize_for_sheets(normalize_point(point))
    ts = now_tz().isoformat(timespec="seconds")

    row, idx, _ = get_user_row_and_index(user_id)
    if row is None:
        sheet_append(SHEET_USERS, [str(user_id), name, point, status, ts, ts])
        return

    created_at = row[4] if len(row) >= 5 else ts
    new_row = [str(user_id), name, point, status, created_at, ts]
    sheet_update(f"{SHEET_USERS}!A{idx}:F{idx}", new_row)


def set_user_status(user_id: int, status: str):
    u = get_user(user_id)
    if not u:
        return
    upsert_user(user_id, u.name, u.point, status=status)


def set_user_point(user_id: int, point: str):
    u = get_user(user_id)
    if not u:
        return
    upsert_user(user_id, u.name, point, status=u.status)


def is_user_active(user_id: int) -> bool:
    u = get_user(user_id)
    return bool(u and u.status == STATUS_ACTIVE)


def is_user_blocked(user_id: int) -> bool:
    u = get_user(user_id)
    return bool(u and u.status == STATUS_BLOCKED)


def is_user_pending(user_id: int) -> bool:
    u = get_user(user_id)
    return bool(u and u.status == STATUS_PENDING)


def list_active_users_by_point(point: str) -> List[UserRec]:
    rows, has_header = _users_rows()
    if not rows:
        return []
    start = 1 if has_header else 0
    out: List[UserRec] = []
    for r in rows[start:]:
        if len(r) < 4:
            continue
        try:
            u = parse_user(r)
        except Exception:
            continue
        if u.status != STATUS_ACTIVE:
            continue
        if normalize_point(u.point) != normalize_point(point):
            continue
        out.append(u)
    return out


def list_active_users_all() -> List[UserRec]:
    """Все активные сотрудники (независимо от выбранной точки)."""
    rows, has_header = _users_rows()
    if not rows:
        return []
    start = 1 if has_header else 0
    out: List[UserRec] = []
    for r in rows[start:]:
        if len(r) < 4:
            continue
        try:
            u = parse_user(r)
        except Exception:
            continue
        if u.status == STATUS_ACTIVE:
            out.append(u)
    return out


# -------------------- TASKS / SCHEDULE --------------------


@dataclass
class Task:
    task_id: str
    task_name: str
    point: str


def _truthy(x: str) -> bool:
    s = (x or "").strip().lower()
    return s in ("1", "true", "yes", "да", "y", "ok")


def load_tasks_for_today(point_selected: str) -> List[Task]:
    """
    Берём из cleaning_schedule задачи, у которых:
    - в колонке D{сегодня} стоит 1/TRUE
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
        if len(r) <= max(2, day_idx):
            continue
        task_id = (r[0] or "").strip() if len(r) > 0 else ""
        task_name = (r[1] or "").strip() if len(r) > 1 else ""
        p = (r[2] or "").strip() if len(r) > 2 else ""
        flag = r[day_idx] if len(r) > day_idx else "0"
        if not task_id or not task_name:
            continue
        if not _truthy(flag):
            continue
        if p == "ALL" or normalize_point(p) == normalize_point(point_selected):
            tasks.append(Task(task_id=task_id, task_name=task_name, point=p))
    return tasks


def split_tasks_half(tasks: List[Task]) -> Tuple[List[Task], List[Task], int]:
    """Делим пополам стабильно по порядку. Возвращаем (part1, part2, split_index)."""
    n = len(tasks)
    split_index = (n + 1) // 2
    return tasks[:split_index], tasks[split_index:], split_index


# -------------------- DONE LOG --------------------


def log_done(day: str, point: str, user: UserRec, task: Task, part: str, photo1: str, photo2: str):
    ts = now_tz().isoformat(timespec="seconds")
    sheet_append(
        SHEET_DONE,
        [
            ts,
            day,
            sanitize_for_sheets(normalize_point(point)),
            str(user.user_id),
            sanitize_for_sheets(user.name),
            sanitize_for_sheets(task.task_id),
            sanitize_for_sheets(task.task_name),
            sanitize_for_sheets(part),
            photo1,
            photo2,
        ],
    )


def get_done_task_ids(day: str, point: str) -> set[str]:
    """Глобально на точке/день: какие task_id уже закрыты (независимо от сотрудника)."""
    try:
        rows = sheet_get(f"{SHEET_DONE}!A2:J")
    except Exception:
        return set()
    out: set[str] = set()
    p = normalize_point(point)
    for r in rows:
        if len(r) < 7:
            continue
        if r[1] != day:
            continue
        if normalize_point(r[2]) != p:
            continue
        tid = r[5] if len(r) > 5 else ""
        if tid:
            out.add(tid)
    return out


def last_task_action_ts(day: str, point: str, user_id: int) -> Optional[datetime]:
    """Последняя отметка задачи этим пользователем на точке/день."""
    try:
        rows = sheet_get(f"{SHEET_DONE}!A2:J")
    except Exception:
        return None
    p = normalize_point(point)
    last: Optional[datetime] = None
    uid = str(user_id)
    for r in rows:
        if len(r) < 4:
            continue
        if r[1] != day:
            continue
        if normalize_point(r[2]) != p:
            continue
        if r[3] != uid:
            continue
        try:
            ts = datetime.fromisoformat(r[0])
        except Exception:
            continue
        if (last is None) or (ts > last):
            last = ts
    return last


# -------------------- SHIFT SESSIONS --------------------


@dataclass
class Session:
    session_id: str
    day: str
    point: str
    mode: str  # FULL | HALF
    state: str  # OPEN_FULL | OPEN1 | WAIT_ACCEPT | OPEN2 | CLOSED
    user1_id: str
    user1_name: str
    user1_start: str
    user1_end: str
    user2_id: str
    user2_name: str
    user2_start: str
    user2_end: str
    split_index: str
    updated_at: str


def make_session_id(day: str, point: str) -> str:
    return f"{day}|{normalize_point(point)}"


def _sessions_rows() -> Tuple[List[List[str]], bool]:
    rows = sheet_get(SHEET_SESSIONS)
    if not rows:
        return [], False
    has_header = is_header(rows[0], "session_id")
    return rows, has_header


def get_session(day: str, point: str) -> Tuple[Optional[Session], Optional[int]]:
    rows, has_header = _sessions_rows()
    if not rows:
        return None, None
    start = 1 if has_header else 0
    sid = make_session_id(day, point)
    for i, r in enumerate(rows[start:], start=1 + start):
        if r and r[0] == sid:
            # pad to header length
            while len(r) < len(SESSIONS_HEADER):
                r.append("")
            try:
                return Session(*r[:len(SESSIONS_HEADER)]), i
            except Exception:
                return None, None
    return None, None


def upsert_session(sess: Session):
    ts = now_tz().isoformat(timespec="seconds")
    sess.updated_at = ts
    existing, idx = get_session(sess.day, sess.point)
    row = list(sess.__dict__.values())
    if existing is None or idx is None:
        sheet_append(SHEET_SESSIONS, row)
    else:
        sheet_update(f"{SHEET_SESSIONS}!A{idx}:O{idx}", row)


def list_open_sessions() -> List[Session]:
    rows, has_header = _sessions_rows()
    if not rows:
        return []
    start = 1 if has_header else 0
    out: List[Session] = []
    for r in rows[start:]:
        if not r:
            continue
        while len(r) < len(SESSIONS_HEADER):
            r.append("")
        try:
            s = Session(*r[:len(SESSIONS_HEADER)])
        except Exception:
            continue
        if s.state and s.state != "CLOSED":
            out.append(s)
    return out


def user_open_context(user_id: int) -> Tuple[Optional[Session], Optional[str]]:
    """Возвращает (session, role) где role: 'FULL', 'HALF1', 'HALF2'."""
    d = day_key()
    sessions = list_open_sessions()
    for s in sessions:
        if s.day != d:
            continue
        if s.mode == "FULL" and s.state == "OPEN_FULL" and s.user1_id == str(user_id):
            return s, "FULL"
        if s.mode == "HALF":
            if s.state == "OPEN1" and s.user1_id == str(user_id):
                return s, "HALF1"
            if s.state == "OPEN2" and s.user2_id == str(user_id):
                return s, "HALF2"
    return None, None


# -------------------- WORK HOURS / CLOSE BUTTON --------------------


WORK_HOURS = {
    "69 Параллель": (time(10, 0), time(22, 0)),
    "Арена": (time(10, 0), time(22, 0)),
    "Музей": (time(9, 0), time(19, 0)),
    "Сочнева": (time(14, 0), time(23, 0)),
}


def point_hours(point: str) -> Tuple[time, time]:
    p = normalize_point(point)
    return WORK_HOURS.get(p, (time(10, 0), time(22, 0)))


def can_close_now(point: str) -> bool:
    _start, end = point_hours(point)
    now = now_tz().time()
    return now >= end


def in_work_hours(point: str) -> bool:
    start, end = point_hours(point)
    now = now_tz().time()
    return start <= now <= end


# -------------------- UI BUILDERS --------------------


def kb_single(label: str, cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=cb)]])


def points_kb(points: List[str], prefix: str = "POINT") -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(p, callback_data=f"{prefix}|{i}")] for i, p in enumerate(points)]
    return InlineKeyboardMarkup(rows)


def after_approved_kb() -> InlineKeyboardMarkup:
    return kb_single("📍 Сменить точку", "CHOOSE_POINT")


def open_choice_kb() -> InlineKeyboardMarkup:
    # Строгая логика: после выбора точки — только 2 кнопки открытия смены
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔓 Открыть смену (полная)", callback_data="OPEN|FULL")],
        [InlineKeyboardButton("⏱️ Открыть пол смены", callback_data="OPEN|HALF")],
    ])


def shift_kb(role: str, point: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🧾 План задач", callback_data="PLAN")],
        [InlineKeyboardButton("✅ Отметить выполненную задачу", callback_data="MARK")],
        [InlineKeyboardButton("🤝 Красавчик помоги", callback_data="HELP")],
    ]
    if role == "HALF1":
        rows.append([InlineKeyboardButton("🔁 Передать смену", callback_data="TRANSFER")])
    if role in ("FULL", "HALF1", "HALF2"):
        rows.append([InlineKeyboardButton("🔒 Закрыть смену", callback_data="CLOSE")])
    return InlineKeyboardMarkup(rows)


def tasks_kb(tasks: List[Task], done_ids: set[str]) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for i, t in enumerate(tasks):
        status = "✅" if t.task_id in done_ids else "⬜"
        label = f"{status} {t.task_name}"
        if len(label) > 48:
            label = label[:45] + "…"
        rows.append([InlineKeyboardButton(label, callback_data=f"TASK|{i}")])
    return InlineKeyboardMarkup(rows)


def approve_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Одобрить", callback_data=f"ADM|APPROVE|{user_id}"),
            InlineKeyboardButton("⛔️ Блок", callback_data=f"ADM|BLOCK|{user_id}"),
        ]
    ])


# -------------------- STATE / MODES (per-user in user_data) --------------------
# Awaiting task photos:
#   await = "TASK_PHOTO1" / "TASK_PHOTO2"
#   task_mark = {point, part, task_id, task_name, photo1, photo2}
#
# Awaiting full shift open photo:
#   await = "OPEN_FULL_PHOTO"
#   open_full_point = ...
#
# Transfer select:
#   transfer_step = "PICK_USER2"
#
# Help:
#   help_mode = True; help_text; help_photos[]
#
# Close shift uses ConversationHandler

# -------------------- REGISTRATION CONV --------------------

REG_NAME, REG_CODE = range(2)

# -------------------- CLOSE SHIFT CONV --------------------

CASH_IN, SALES_CASHLESS, SALES_CASH, REFUNDS, RECEIPT1, RECEIPT2, CLEANUP = range(7)

# -------------------- OPEN FULL SHIFT CONV --------------------

OPEN_FULL_REPORT, OPEN_FULL_SHOWCASE, OPEN_FULL_MACARONS = range(3)



def parse_money(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(" ", "").replace(",", ".")
    try:
        v = float(s)
        if v < 0:
            return None
        return v
    except Exception:
        return None


async def guard_employee(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[UserRec]:
    """Единая проверка доступа для сотрудников (не для админ-команд в группе)."""
    uid = update.effective_user.id if update.effective_user else 0
    u = get_user(uid)
    if not u:
        # не зарегистрирован
        if update.message:
            await update.message.reply_text("Сначала регистрация: /start")
        elif update.callback_query:
            await update.callback_query.answer()
            await safe_edit(update.callback_query, "Сначала регистрация: /start")
        return None
    if u.status == STATUS_BLOCKED:
        if update.message:
            await update.message.reply_text("Доступ к боту заблокирован администратором.")
        elif update.callback_query:
            await update.callback_query.answer()
            await safe_edit(update.callback_query, "Доступ к боту заблокирован администратором.")
        return None
    if u.status == STATUS_PENDING:
        if update.message:
            await update.message.reply_text("Ты уже отправил заявку. Ждём одобрения в группе контроля 🙂")
        elif update.callback_query:
            await update.callback_query.answer()
            await safe_edit(update.callback_query, "Заявка на одобрении. Ждём 🙂")
        return None
    return u


# -------------------- HANDLERS: START / REGISTER --------------------


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    u = get_user(uid)

    if u and u.status == STATUS_BLOCKED:
        await update.message.reply_text("Доступ к боту заблокирован администратором.")
        return ConversationHandler.END

    if u and u.status == STATUS_PENDING:
        await update.message.reply_text("Заявка уже отправлена. Ждём одобрения в группе контроля 🙂")
        return ConversationHandler.END

    if u and u.status == STATUS_ACTIVE:
        # знакомый
        text = "А я тебя помню! 🙂"
        sess, role = user_open_context(uid)
        if sess and role:
            point = normalize_point(sess.point)
            await update.message.reply_text(text + f"\n\nСмена уже открыта на точке: {point}", reply_markup=shift_kb(role, point))
            return ConversationHandler.END
        if not u.point:
            await update.message.reply_text(text + "\n\nВыбери точку:", reply_markup=after_approved_kb())
        else:
            await update.message.reply_text(text + f"\n\nТвоя точка сейчас: {normalize_point(u.point)}\nНажми «Сменить точку» (можно выбрать ту же) и потом открой смену.", reply_markup=after_approved_kb())
        return ConversationHandler.END

    # новая регистрация
    await update.message.reply_text(
        "Привет! Давай зарегистрируемся.\n\nНапиши своё имя:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return REG_NAME


async def reg_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = normalize_name(update.message.text)
    if len(name) < 2:
        await update.message.reply_text("Имя слишком короткое. Напиши хотя бы 2 буквы.")
        return REG_NAME
    context.user_data["reg_name"] = name
    await update.message.reply_text("Теперь введи код доступа:")
    return REG_CODE


async def reg_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = (update.message.text or "").strip()
    if code != ACCESS_CODE:
        await update.message.reply_text("Код неверный. Попробуй ещё раз:")
        return REG_CODE

    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.full_name)

    upsert_user(uid, name, point="", status=STATUS_PENDING)

    await update.message.reply_text(
        "Заявка отправлена в группу контроля ✅\n"
        "Как только одобрят — я напишу тебе сюда.",
    )

    # в контроль
    await report_to_control(
        context,
        format_control("🆕 Запрос регистрации", name, uid, details=["Нажмите кнопку ниже:"]),
    )
    try:
        await context.bot.send_message(
            chat_id=CONTROL_GROUP_ID,
            text=f"🆕 Запрос регистрации\nИмя: {name}\nID: {uid}\n\nОдобрить?",
            reply_markup=approve_kb(uid),
        )
    except Exception as e:
        log.warning("Не смог отправить approval-кнопки: %s", e)

    return ConversationHandler.END


# -------------------- ADMIN: APPROVE/BLOCK CALLBACKS --------------------


def _is_control_chat(update: Update) -> bool:
    if update.callback_query and update.callback_query.message:
        return update.callback_query.message.chat_id == CONTROL_GROUP_ID
    if update.message:
        return update.message.chat_id == CONTROL_GROUP_ID
    return False


async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if not _is_control_chat(update):
        await safe_edit(q, "Эти кнопки работают только в группе контроля.")
        return

    try:
        _p, action, uid_s = q.data.split("|", 2)
        uid = int(uid_s)
    except Exception:
        await safe_edit(q, "Некорректная команда.")
        return

    u = get_user(uid)
    if not u:
        await safe_edit(q, "Пользователь не найден в таблице users.")
        return

    if action == "APPROVE":
        set_user_status(uid, STATUS_ACTIVE)
        await safe_edit(q, f"✅ Одобрено: {u.name} ({uid})")

        # уведомить сотрудника
        try:
            await context.bot.send_message(
                chat_id=uid,
                text="✅ Тебя одобрили!\nТеперь выбери точку (можно менять в любой момент, когда смена закрыта):",
                reply_markup=after_approved_kb(),
            )
        except Exception as e:
            log.warning("Не смог написать пользователю после approve: %s", e)

        await report_to_control(context, format_control("✅ Сотрудник одобрен", u.name, uid))

    elif action == "BLOCK":
        set_user_status(uid, STATUS_BLOCKED)
        await safe_edit(q, f"⛔️ Заблокирован: {u.name} ({uid})")
        try:
            await context.bot.send_message(chat_id=uid, text="⛔️ Доступ к боту заблокирован администратором.")
        except Exception:
            pass
        await report_to_control(context, format_control("⛔️ Сотрудник заблокирован", u.name, uid))


# -------------------- ADMIN COMMANDS (control group only) --------------------


async def cmd_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.chat_id != CONTROL_GROUP_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /block <user_id>")
        return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("user_id должен быть числом.")
        return
    u = get_user(uid)
    if not u:
        await update.message.reply_text("Не найден в users.")
        return
    set_user_status(uid, STATUS_BLOCKED)
    await update.message.reply_text(f"⛔️ Заблокирован: {u.name} ({uid})")
    try:
        await context.bot.send_message(chat_id=uid, text="⛔️ Доступ к боту заблокирован администратором.")
    except Exception:
        pass


async def cmd_unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.chat_id != CONTROL_GROUP_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /unblock <user_id>")
        return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("user_id должен быть числом.")
        return
    u = get_user(uid)
    if not u:
        await update.message.reply_text("Не найден в users.")
        return
    # если был заблокирован — делаем активным (если был pending — оставим pending)
    new_status = STATUS_ACTIVE if u.status == STATUS_BLOCKED else u.status
    set_user_status(uid, new_status)
    await update.message.reply_text(f"✅ Разблокирован: {u.name} ({uid}), статус: {new_status}")


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.chat_id != CONTROL_GROUP_ID:
        return
    rows, has_header = _users_rows()
    if not rows:
        await update.message.reply_text("users пустой.")
        return
    start = 1 if has_header else 0
    pending: List[UserRec] = []
    for r in rows[start:]:
        if len(r) < 4:
            continue
        try:
            u = parse_user(r)
        except Exception:
            continue
        if u.status == STATUS_PENDING:
            pending.append(u)
    if not pending:
        await update.message.reply_text("На одобрении никого нет.")
        return
    lines = ["На одобрении:"]
    for u in pending[:40]:
        lines.append(f"• {u.name} — {u.user_id}")
    await update.message.reply_text("\n".join(lines))


# -------------------- EMPLOYEE: POINT / OPEN --------------------


async def choose_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return

    # Строгая логика: если смена уже открыта — выбор точки запрещён
    sess, role = user_open_context(u.user_id)
    if sess and role:
        point = normalize_point(sess.point)
        await safe_edit(q, "Смена уже открыта. Действуй по кнопкам ниже.", reply_markup=shift_kb(role, point))
        return

    pts = load_points()
    context.user_data["points_list"] = pts
    await safe_edit(q, "Выбери точку:", reply_markup=points_kb(pts, prefix="POINT"))


async def point_pick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return

    # Строгая логика: если смена уже открыта — смена точки запрещена
    sess, role = user_open_context(u.user_id)
    if sess and role:
        point = normalize_point(sess.point)
        await safe_edit(q, "Смена уже открыта. Сменить точку нельзя.", reply_markup=shift_kb(role, point))
        return

    pts = context.user_data.get("points_list") or load_points()
    try:
        _p, idx_s = q.data.split("|", 1)
        point = pts[int(idx_s)]
    except Exception:
        await safe_edit(q, "Не понял выбор. Нажми «Выбор точки» ещё раз.", reply_markup=after_approved_kb())
        return

    set_user_point(u.user_id, point)
    u = get_user(u.user_id) or u

    await safe_edit(q, f"Точка выбрана: {normalize_point(point)}\n\nТеперь выбери вариант открытия смены:", reply_markup=open_choice_kb())
    await report_to_control(context, format_control("📍 Сотрудник выбрал точку", u.name, u.user_id, point=normalize_point(point)))


async def back_to_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return
    await safe_edit(q, "Выбери точку:", reply_markup=after_approved_kb())


async def open_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return

    if not u.point:
        await safe_edit(q, "Сначала выбери точку:", reply_markup=after_approved_kb())
        return

    point = normalize_point(u.point)
    d = day_key()

    # mode: FULL or HALF
    try:
        _p, mode = (q.data or "").split("|", 1)
    except Exception:
        mode = "FULL"
    if mode not in ("FULL", "HALF"):
        mode = "FULL"
    context.user_data["open_shift_mode"] = mode
    existing, _ = get_session(d, point)
    _, role = user_open_context(u.user_id)
    if role:
        await safe_edit(q, "У тебя уже есть открытая смена.", reply_markup=shift_kb(role, point))
        return

    try:
        _p, mode = q.data.split("|", 1)
    except Exception:
        await safe_edit(q, "Некорректная команда.")
        return

    if existing and existing.state != "CLOSED":
        # Уже есть смена на точке сегодня
        if existing.mode == "FULL":
            await safe_edit(q, "На этой точке уже открыта полная смена сегодня. Обратись к руководителю.", reply_markup=open_choice_kb())
            return
        if existing.mode == "HALF":
            await safe_edit(q, "На этой точке уже идёт пол-смены сегодня. Обратись к руководителю.", reply_markup=open_choice_kb())
            return

    if mode == "FULL":
        # Полная смена открывается через сценарий: отчет -> фото витрины -> фото макаронс
        await safe_edit(q, "Полная смена: сначала отчёт витрины, затем 2 фото. Пожалуйста, нажми кнопку ещё раз.")
        return

    if mode == "HALF":
        # Пол-смена открывается через тот же сценарий, что и полная: отчет -> фото витрины -> фото макаронс
        await safe_edit(q, "Пол смены: сначала отчёт витрины, затем 2 фото. Пожалуйста, нажми кнопку ещё раз.")
        return


# -------------------- OPEN FULL SHIFT (TEXT -> PHOTO -> PHOTO) --------------------

async def open_full_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    if not u.point:
        await safe_edit(q, "Сначала выбери точку:", reply_markup=after_approved_kb())
        return ConversationHandler.END

    point = normalize_point(u.point)
    d = day_key()

    # mode: FULL or HALF (OPEN|FULL / OPEN|HALF)
    try:
        _p, mode = (q.data or "").split("|", 1)
    except Exception:
        mode = "FULL"
    if mode not in ("FULL", "HALF"):
        mode = "FULL"
    context.user_data["open_shift_mode"] = mode

    # если у пользователя уже есть открытая смена — запрещаем
    sess_open, role = user_open_context(u.user_id)
    if role:
        p = normalize_point(sess_open.point) if sess_open else point
        await safe_edit(q, "У тебя уже есть открытая смена.", reply_markup=shift_kb(role, p))
        return ConversationHandler.END

    existing, _ = get_session(d, point)
    if existing and existing.state != "CLOSED":
        if existing.mode == "FULL":
            await safe_edit(q, 
                "На этой точке уже открыта полная смена сегодня. Обратись к руководителю.",
                reply_markup=open_choice_kb(),
            )
        else:
            await safe_edit(q, 
                "На этой точке уже идёт пол-смены сегодня. Обратись к руководителю.",
                reply_markup=open_choice_kb(),
            )
        return ConversationHandler.END

    # старт сценария
    context.user_data["open_full_point"] = point
    context.user_data["open_full_day"] = d
    context.user_data.pop("open_full_report", None)
    context.user_data.pop("open_full_photo_showcase", None)
    context.user_data.pop("open_full_photo_macarons", None)

    label = "Пол смены" if context.user_data.get("open_shift_mode") == "HALF" else "Полная смена"
    await safe_edit(q, 
        f"{label}.\n\n"
        "Перечисли десерты в витрине и сроки их годности:",
    )
    return OPEN_FULL_REPORT


async def open_full_report_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    text = (update.message.text or "").strip()
    if len(text) < 3:
        await update.message.reply_text("Слишком коротко 🙂 Напиши списком десерты и сроки годности.")
        return OPEN_FULL_REPORT

    context.user_data["open_full_report"] = text
    await update.message.reply_text(
        "Отчет принят ✅\n\n"
        "Теперь пришли фото витрины 📸",
    )
    return OPEN_FULL_SHOWCASE


async def open_full_need_showcase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Нужно фото витрины 📸 Пришли фотографию, пожалуйста.")
    return OPEN_FULL_SHOWCASE


async def open_full_need_macarons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Нужно фото макаронс 📸 Пришли фотографию, пожалуйста.")
    return OPEN_FULL_MACARONS


async def open_full_showcase_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    file_id = _extract_photo_file_id(update)
    if not file_id:
        await update.message.reply_text("Нужно фото витрины 📸")
        return OPEN_FULL_SHOWCASE

    context.user_data["open_full_photo_showcase"] = file_id
    await update.message.reply_text(
        "Фото витрины принято ✅\n\n"
        "Теперь пришли фото макаронс со сроком годности и вкусами 📸",
    )
    return OPEN_FULL_MACARONS


async def open_full_macarons_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    file_id = _extract_photo_file_id(update)
    if not file_id:
        await update.message.reply_text("Нужно фото макаронс 📸")
        return OPEN_FULL_MACARONS

    point = context.user_data.get("open_full_point") or normalize_point(u.point)
    d = context.user_data.get("open_full_day") or day_key()

    # защитная проверка: на всякий случай
    existing, _ = get_session(d, point)
    if existing and existing.state != "CLOSED":
        context.user_data.pop("open_full_point", None)
        context.user_data.pop("open_full_day", None)
        context.user_data.pop("open_full_report", None)
        context.user_data.pop("open_full_photo_showcase", None)
        context.user_data.pop("open_full_photo_macarons", None)
        await update.message.reply_text("Смена на точке уже открыта. Меню:", reply_markup=open_choice_kb())
        return ConversationHandler.END

    context.user_data["open_full_photo_macarons"] = file_id

    report_text = (context.user_data.get("open_full_report") or "").strip()
    photo_showcase = context.user_data.get("open_full_photo_showcase") or ""
    photo_macarons = context.user_data.get("open_full_photo_macarons") or ""

    ts = now_tz().isoformat(timespec="seconds")
    mode = context.user_data.get("open_shift_mode") or "FULL"
    if mode == "HALF":
        # половина смены: делим задачи и открываем OPEN1
        tasks = load_tasks_for_today(point)
        _part1, _part2, split_index = split_tasks_half(tasks)
        sess = Session(
            session_id=make_session_id(d, point),
            day=d,
            point=point,
            mode="HALF",
            state="OPEN1",
            user1_id=str(u.user_id),
            user1_name=u.name,
            user1_start=ts,
            user1_end="",
            user2_id="",
            user2_name="",
            user2_start="",
            user2_end="",
            split_index=str(split_index),
            updated_at=ts,
        )
    else:
        sess = Session(
            session_id=make_session_id(d, point),
            day=d,
            point=point,
            mode="FULL",
            state="OPEN_FULL",
            user1_id=str(u.user_id),
            user1_name=u.name,
            user1_start=ts,
            user1_end="",
            user2_id="",
            user2_name="",
            user2_start="",
            user2_end="",
            split_index="",
            updated_at=ts,
        )
    upsert_session(sess)

    # очистка временных полей открытия
    context.user_data.pop("open_full_point", None)
    context.user_data.pop("open_full_day", None)
    context.user_data.pop("open_full_report", None)
    context.user_data.pop("open_full_photo_showcase", None)
    context.user_data.pop("open_full_photo_macarons", None)

    # отчет в контроль: открытие + текст + 2 фото
    details = [f"Время: {ts}"]
    if report_text:
        details.append("Отчет витрины:")
        details.append(report_text[:1500])

    await report_to_control(
        context,
        format_control(
            ("⏱️ Открыта пол смены" if mode == "HALF" else "🔓 Открыта смена (полная)"),
            u.name,
            u.user_id,
            point=point,
            details=details,
        ),
    )

    if photo_showcase:
        cap = f"📸 Витрина (готовность)\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})"
        if report_text:
            cap += f"\n\nОтчет:\n{report_text[:800]}"
        await report_photo_to_control(context, photo_showcase, caption=cap)

    if photo_macarons:
        await report_photo_to_control(
            context,
            photo_macarons,
            caption=f"📸 Макаронс (срок годности и вкусы)\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})",
        )

    await update.message.reply_text(
        f"Смена открыта ✅\nТочка: {point}",
        reply_markup=shift_kb("HALF1", point) if mode == "HALF" else shift_kb("FULL", point),
    )
    context.user_data.pop("open_shift_mode", None)
    return ConversationHandler.END

# -------------------- PHOTO MESSAGE HANDLER (task/open/help) --------------------


def _extract_photo_file_id(update: Update) -> Optional[str]:
    if update.message and update.message.photo:
        return update.message.photo[-1].file_id
    if update.message and update.message.document and update.message.document.mime_type:
        if update.message.document.mime_type.startswith("image/"):
            return update.message.document.file_id
    return None


async def photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = await guard_employee(update, context)
    if not u:
        return

    file_id = _extract_photo_file_id(update)
    if not file_id:
        return
    # OPEN FULL PHOTO
    # (открытие полной смены теперь идёт через ConversationHandler open_full_conv)

    # TASK PHOTOS
    if context.user_data.get("await") in ("TASK_PHOTO1", "TASK_PHOTO2"):
        task_mark = context.user_data.get("task_mark") or {}
        if not task_mark:
            context.user_data.pop("await", None)
            await update.message.reply_text("Я потерял контекст задачи 😅 Нажми «Отметить выполненную задачу» ещё раз.")
            return

        if context.user_data["await"] == "TASK_PHOTO1":
            task_mark["photo1"] = file_id
            context.user_data["task_mark"] = task_mark
            context.user_data["await"] = "TASK_PHOTO2"
            await update.message.reply_text(
                "Фото 1 принято ✅\n\n"
                "Теперь пришли фото 2 (по желанию) 📸\n"
                "или нажми «Пропустить».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить фото 2", callback_data="SKIP_TASK_PHOTO2")]]),
            )
            return

        if context.user_data["await"] == "TASK_PHOTO2":
            task_mark["photo2"] = file_id
            context.user_data["task_mark"] = task_mark
            # финализируем
            await finalize_task_done(update, context, u, task_mark)
            return

    # HELP MODE photos
    if context.user_data.get("help_mode"):
        photos: List[str] = context.user_data.get("help_photos") or []
        if len(photos) >= 4:
            await update.message.reply_text("Уже 4 фото. Нажми «Отправить» 🙂")
            return
        photos.append(file_id)
        context.user_data["help_photos"] = photos
        left = 4 - len(photos)
        await update.message.reply_text(
            f"Фото добавлено ✅ (осталось до 4: {left})\nНажми «Отправить», когда закончишь.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Отправить", callback_data="HELP_SEND")],
                [InlineKeyboardButton("❌ Отмена", callback_data="HELP_CANCEL")],
            ]),
        )
        return

    # Вне сценария — мягко игнорируем
    await update.message.reply_text("Фото получено 👍 Но сейчас я его ни для чего не жду.\nОткрой меню и действуй по кнопкам.")


# -------------------- TASK FLOW --------------------


def assigned_tasks_for_user(sess: Session, role: str, point: str) -> Tuple[List[Task], str]:
    """Возвращает (tasks_for_user, part_label). part_label: FULL | HALF1 | HALF2"""
    tasks = load_tasks_for_today(point)
    if role == "FULL":
        return tasks, "FULL"
    if role == "HALF1":
        split_index = int(sess.split_index or "0")
        return tasks[:split_index], "HALF1"
    if role == "HALF2":
        split_index = int(sess.split_index or "0")
        return tasks[split_index:], "HALF2"
    return [], role


async def plan_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return

    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        await safe_edit(q, "Смена не открыта. Выбери точку и открой смену.", reply_markup=open_choice_kb())
        return

    point = normalize_point(sess.point)
    day = sess.day
    tasks, _part = assigned_tasks_for_user(sess, role, point)
    done_ids = get_done_task_ids(day, point)

    if not tasks:
        await safe_edit(q, "На сегодня задач нет 🙂", reply_markup=shift_kb(role, point))
        return

    lines = [f"План задач ({day}, {point}):"]
    for t in tasks:
        status = "✅" if t.task_id in done_ids else "⬜"
        lines.append(f"{status} {t.task_name}")

    await safe_edit(q, "\n".join(lines), reply_markup=shift_kb(role, point))


async def mark_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return

    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        await safe_edit(q, "Смена не открыта.", reply_markup=open_choice_kb())
        return

    point = normalize_point(sess.point)
    day = sess.day
    tasks, part = assigned_tasks_for_user(sess, role, point)

    if not tasks:
        await safe_edit(q, "Сегодня нечего отмечать 🙂", reply_markup=shift_kb(role, point))
        return

    done_ids = get_done_task_ids(day, point)
    remaining = [t for t in tasks if t.task_id not in done_ids]

    if not remaining:
        await safe_edit(q, "Все твои задачи уже отмечены ✅", reply_markup=shift_kb(role, point))
        return

    context.user_data["mark_list"] = [{"task_id": t.task_id, "task_name": t.task_name} for t in remaining]
    context.user_data["mark_point"] = point
    context.user_data["mark_part"] = part
    await safe_edit(q, "Что выполнено? Нажми задачу:", reply_markup=tasks_kb(remaining, done_ids=set()))


async def task_pick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return

    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        await safe_edit(q, "Смена не открыта.", reply_markup=open_choice_kb())
        return

    mark_list = context.user_data.get("mark_list") or []
    try:
        _p, idx_s = q.data.split("|", 1)
        item = mark_list[int(idx_s)]
    except Exception:
        await safe_edit(q, "Не понял выбор. Нажми «Отметить выполненную задачу» ещё раз.", reply_markup=shift_kb(role, normalize_point(sess.point)))
        return

    point = context.user_data.get("mark_point") or normalize_point(sess.point)
    part = context.user_data.get("mark_part") or role
    day = sess.day

    # защита от повторов (если кто-то уже отметил)
    done_ids = get_done_task_ids(day, point)
    if item["task_id"] in done_ids:
        await safe_edit(q, "Эта задача уже отмечена ✅", reply_markup=shift_kb(role, point))
        return

    context.user_data["task_mark"] = {
        "day": day,
        "point": point,
        "part": part,
        "task_id": item["task_id"],
        "task_name": item["task_name"],
        "photo1": "",
        "photo2": "",
    }
    context.user_data["await"] = "TASK_PHOTO1"

    await safe_edit(q, 
        f"Задача: {item['task_name']}\n\n"
        "Пришли фото 1 (обязательно) 📸",    )


async def skip_task_photo2_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return

    task_mark = context.user_data.get("task_mark") or {}
    if not task_mark or not task_mark.get("photo1"):
        await safe_edit(q, "Сначала нужно прислать фото 1 🙂")
        return

    await finalize_task_done(update, context, u, task_mark, via_callback=True)


async def finalize_task_done(update: Update, context: ContextTypes.DEFAULT_TYPE, user: UserRec, task_mark: Dict[str, Any], via_callback: bool = False):
    day = task_mark["day"]
    point = task_mark["point"]
    part = task_mark["part"]
    task = Task(task_id=task_mark["task_id"], task_name=task_mark["task_name"], point=point)
    photo1 = task_mark.get("photo1", "")
    photo2 = task_mark.get("photo2", "")

    # лог в таблицу
    log_done(day, point, user, task, part, photo1, photo2)

    # reset throttling ONLY when a task is marked done
    try:
        flag = f"reminder_sent:{day}:{normalize_point(point)}:{user.user_id}"
        context.bot_data[flag] = now_tz().isoformat(timespec="seconds")
    except Exception:
        pass

    # очистка состояния
    context.user_data.pop("await", None)
    context.user_data.pop("task_mark", None)

    # контроль: сообщение + фото
    await report_to_control(
        context,
        format_control(
            "✅ Задача выполнена",
            user.name,
            user.user_id,
            point=point,
            details=[f"Задача: {task.task_name}", f"Часть смены: {part}"],
        ),
    )
    if photo1:
        await report_photo_to_control(context, photo1, caption=f"📸 Отчет 1\nТочка: {point}\nЗадача: {task.task_name}\nСотрудник: {user.name} ({user.user_id})")
    if photo2:
        await report_photo_to_control(context, photo2, caption=f"📸 Отчет 2\nТочка: {point}\nЗадача: {task.task_name}\nСотрудник: {user.name} ({user.user_id})")

    # вернуть меню смены
    sess, role = user_open_context(user.user_id)
    if sess and role:
        text = f"Готово ✅\nОтметил: {task.task_name}"
        if via_callback and update.callback_query:
            try:
                await safe_edit(update.callback_query, text, reply_markup=shift_kb(role, normalize_point(sess.point)))
                return
            except Exception:
                pass
        await (update.effective_message.reply_text(text, reply_markup=shift_kb(role, normalize_point(sess.point))))


# -------------------- HELP FLOW --------------------


async def help_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return

    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        await safe_edit(q, "Кнопка доступна только в рамках открытой смены.")
        return

    point = normalize_point(sess.point)
    context.user_data["help_mode"] = True
    context.user_data["help_point"] = point
    context.user_data["help_photos"] = []
    context.user_data.pop("help_text", None)

    await safe_edit(q, 
        "Надеюсь новости хорошие!? 🙂\n"
        "Напиши всё что хочешь сказать и прикрепи фото если нужно.\n\n"
        "Сначала отправь ТЕКСТ одним сообщением.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="HELP_CANCEL")]]),
    )


async def help_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("help_mode"):
        return
    if context.user_data.get("help_text"):
        return
    text = (update.message.text or "").strip()
    if not text:
        return
    context.user_data["help_text"] = text
    await update.message.reply_text(
        "Текст принял ✅\n\nТеперь можешь отправить до 4 фото (по одному или альбомом).\n"
        "Когда закончишь — нажми «Отправить».",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Отправить", callback_data="HELP_SEND")],
            [InlineKeyboardButton("✅ Отправить без фото", callback_data="HELP_SEND")],
            [InlineKeyboardButton("❌ Отмена", callback_data="HELP_CANCEL")],
        ]),
    )


async def help_send_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return

    if not context.user_data.get("help_mode"):
        await safe_edit(q, "Нет активного запроса.")
        return

    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        context.user_data.pop("help_mode", None)
        await safe_edit(q, "Смена не открыта, сообщение не отправлено.")
        return

    point = context.user_data.get("help_point") or normalize_point(sess.point)
    text = context.user_data.get("help_text") or "(без текста)"
    photos: List[str] = context.user_data.get("help_photos") or []

    await report_to_control(
        context,
        format_control(
            "🤝 Красавчик помоги",
            u.name,
            u.user_id,
            point=point,
            details=[f"Сообщение: {text}"],
        ),
    )
    for i, pid in enumerate(photos[:4], start=1):
        await report_photo_to_control(context, pid, caption=f"📸 Фото {i}\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")

    context.user_data.pop("help_mode", None)
    context.user_data.pop("help_text", None)
    context.user_data.pop("help_photos", None)
    context.user_data.pop("help_point", None)

    await safe_edit(q, "Отправил в группу контроля ✅", reply_markup=shift_kb(role, point))


async def help_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    context.user_data.pop("help_mode", None)
    context.user_data.pop("help_text", None)
    context.user_data.pop("help_photos", None)
    context.user_data.pop("help_point", None)

    u = await guard_employee(update, context)
    if not u:
        return

    sess, role = user_open_context(u.user_id)
    if sess and role:
        await safe_edit(q, "Ок, отменил.", reply_markup=shift_kb(role, normalize_point(sess.point)))
    else:
        await safe_edit(q, "Ок, отменил.", reply_markup=open_choice_kb())


# -------------------- BACK BUTTONS --------------------


async def back_main_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return
    if not u.point:
        await safe_edit(q, "Меню:", reply_markup=after_approved_kb())
        return
    await safe_edit(q, "Меню:", reply_markup=open_choice_kb())


async def back_shift_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return
    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        await safe_edit(q, "Смена не открыта.", reply_markup=open_choice_kb())
        return
    await safe_edit(q, "Меню смены:", reply_markup=shift_kb(role, normalize_point(sess.point)))


# -------------------- TRANSFER HALF SHIFT --------------------


async def transfer_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return
    sess, role = user_open_context(u.user_id)
    if not sess or role != "HALF1":
        await safe_edit(q, "Кнопка доступна только первому сотруднику пол-смены.")
        return
    point = normalize_point(sess.point)

    # список активных сотрудников на этой точке
    users = [x for x in list_active_users_all() if x.user_id != u.user_id]
    if not users:
        await safe_edit(q, 
            "Нет активных сотрудников на этой точке для передачи.\n"
            "Пусть второй сотрудник пройдёт регистрацию и выберет эту же точку.",
            reply_markup=shift_kb(role, point),
        )
        return

    rows = []
    for x in users[:30]:
        label = f"{x.name} ({x.user_id})"
        rows.append([InlineKeyboardButton(label, callback_data=f"U2|{x.user_id}")])

    context.user_data["transfer_session_id"] = sess.session_id
    await safe_edit(q, "Кому передаём смену?", reply_markup=InlineKeyboardMarkup(rows))


async def pick_user2_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return

    sess, role = user_open_context(u.user_id)
    if not sess or role != "HALF1":
        await safe_edit(q, "Сейчас ты не в режиме передачи пол-смены.")
        return

    point = normalize_point(sess.point)

    try:
        _p, uid_s = q.data.split("|", 1)
        uid2 = int(uid_s)
    except Exception:
        await safe_edit(q, "Некорректный выбор.", reply_markup=shift_kb(role, point))
        return

    u2 = get_user(uid2)
    if not u2 or u2.status != STATUS_ACTIVE:
        await safe_edit(q, "Этот сотрудник сейчас не активен.", reply_markup=shift_kb(role, point))
        return

    # проверка косяков по задачам первой половины
    tasks_all = load_tasks_for_today(point)
    split_index = int(sess.split_index or "0")
    my_tasks = tasks_all[:split_index]
    done_ids = get_done_task_ids(sess.day, point)
    missing = [t.task_name for t in my_tasks if t.task_id not in done_ids]

    if missing:
        warn = "Лично претензий к тебе нет, но косячек с тебя снял! Руководитель будет крайне не доволен!😌\n" \
               "Задания на сегодня тобою не выполнены."
        await context.bot.send_message(chat_id=u.user_id, text=warn)
        await report_to_control(
            context,
            format_control(
                "⚠️ Косяк при передаче смены (пол смены)",
                u.name,
                u.user_id,
                point=point,
                details=["Не выполнены задачи первой половины:"] + [f"• {x}" for x in missing[:25]],
            ),
        )

    # фиксируем конец у user1 и ставим ожидание
    ts = now_tz().isoformat(timespec="seconds")
    sess.state = "WAIT_ACCEPT"
    sess.user1_end = ts
    sess.user2_id = str(u2.user_id)
    sess.user2_name = u2.name
    upsert_session(sess)

    # отправить запрос принятия user2
    try:
        await context.bot.send_message(
            chat_id=u2.user_id,
            text=f"Тебе передают смену на точке: {point}\nНажми «Принять смену». (Точку выбирать не нужно)",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Принять смену", callback_data=f"ACCEPT|{sess.session_id}")]
            ]),
        )
    except Exception as e:
        log.warning("Не смог отправить accept user2: %s", e)

    await report_to_control(
        context,
        format_control(
            "🔁 Передача смены запрошена",
            u.name,
            u.user_id,
            point=point,
            details=[f"Кому: {u2.name} ({u2.user_id})", f"Время: {ts}"],
        ),
    )

    await safe_edit(q, 
        "Смену передал ✅\n"
        "Второй сотрудник должен нажать «Принять смену».",
        reply_markup=open_choice_kb(),
    )


async def accept_shift_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return

    try:
        _p, session_id = q.data.split("|", 1)
    except Exception:
        await safe_edit(q, "Некорректная команда.")
        return

    # найти сессию по day/point из session_id
    try:
        d, point = session_id.split("|", 1)
    except Exception:
        await safe_edit(q, "Некорректный session_id.")
        return

    sess, _idx = get_session(d, point)
    if not sess or sess.session_id != session_id:
        await safe_edit(q, "Смена не найдена или уже закрыта.")
        return
    if sess.mode != "HALF" or sess.state != "WAIT_ACCEPT":
        await safe_edit(q, "Сейчас нельзя принять эту смену.")
        return
    if sess.user2_id != str(u.user_id):
        await safe_edit(q, "Эта смена адресована другому сотруднику.")
        return


    # Автоматически привязываем сотрудника ко входящей точке смены
    set_user_point(u.user_id, normalize_point(sess.point))

    ts = now_tz().isoformat(timespec="seconds")
    sess.state = "OPEN2"
    sess.user2_start = ts
    upsert_session(sess)

    await report_to_control(
        context,
        format_control(
            "✅ Смена принята (пол смены)",
            u.name,
            u.user_id,
            point=normalize_point(sess.point),
            details=[f"Время: {ts}", f"От кого: {sess.user1_name} ({sess.user1_id})"],
        ),
    )

    await safe_edit(q, 
        f"Смена принята ✅\nТочка: {normalize_point(sess.point)}",
        reply_markup=shift_kb("HALF2", normalize_point(sess.point)),
    )


# -------------------- CLOSE SHIFT CONVERSATION --------------------


async def close_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        await safe_edit(q, "Смена не открыта.")
        return ConversationHandler.END

    point = normalize_point(sess.point)

    if role not in ("FULL", "HALF1", "HALF2"):
        await safe_edit(q, "Закрытие смены доступно только для полной смены или сотрудников пол-смены.")
        return ConversationHandler.END

    # подготовим контекст закрытия
    context.user_data["close"] = {
        "session_id": sess.session_id,
        "day": sess.day,
        "point": point,
        "mode": sess.mode,
        "role": role,
        "user_id": u.user_id,
        "user_name": u.name,
        "cash_in": None,
        "sales_cashless": None,
        "sales_cash": None,
        "refunds": None,
        "receipt1": "",
        "receipt2": "",
        "cleanup": [],
    }

    await safe_edit(q, "Закрытие смены.\n\nВведи наличные в начале смены (внесение):")
    return CASH_IN


async def close_cash_in(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = parse_money(update.message.text)
    if v is None:
        await update.message.reply_text("Не понял сумму. Введи числом, например: 1500 или 1500.50")
        return CASH_IN
    context.user_data["close"]["cash_in"] = v
    await update.message.reply_text("Теперь введи продажи по безналу:")
    return SALES_CASHLESS


async def close_sales_cashless(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = parse_money(update.message.text)
    if v is None:
        await update.message.reply_text("Не понял сумму. Введи числом.")
        return SALES_CASHLESS
    context.user_data["close"]["sales_cashless"] = v
    await update.message.reply_text("Теперь введи продажи по наличке:")
    return SALES_CASH


async def close_sales_cash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = parse_money(update.message.text)
    if v is None:
        await update.message.reply_text("Не понял сумму. Введи числом.")
        return SALES_CASH
    context.user_data["close"]["sales_cash"] = v
    await update.message.reply_text("Теперь введи возвраты:")
    return REFUNDS


async def close_refunds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = parse_money(update.message.text)
    if v is None:
        await update.message.reply_text("Не понял сумму. Введи числом.")
        return REFUNDS
    context.user_data["close"]["refunds"] = v
    await update.message.reply_text("Пришли фото 1 чека закрытия смены 📸")
    return RECEIPT1


async def close_receipt1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = get_user(update.effective_user.id)
    if not u or u.status != STATUS_ACTIVE:
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END

    file_id = _extract_photo_file_id(update)
    if not file_id:
        await update.message.reply_text("Нужна фотография.")
        return RECEIPT1

    context.user_data["close"]["receipt1"] = file_id
    await update.message.reply_text("Принял ✅ Теперь пришли фото 2 чека закрытия смены 📸")
    return RECEIPT2


async def close_receipt2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = get_user(update.effective_user.id)
    if not u or u.status != STATUS_ACTIVE:
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END

    file_id = _extract_photo_file_id(update)
    if not file_id:
        await update.message.reply_text("Нужна фотография.")
        return RECEIPT2

    context.user_data["close"]["receipt2"] = file_id
    await update.message.reply_text(
        "Принял ✅\n\nТеперь пришли 4 фото убранного рабочего места и инвентаря (по одному сообщению). Фото 1/4 📸"
    )
    context.user_data["close"]["cleanup"] = []
    return CLEANUP


async def close_cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = get_user(update.effective_user.id)
    if not u or u.status != STATUS_ACTIVE:
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END

    file_id = _extract_photo_file_id(update)
    if not file_id:
        await update.message.reply_text("Нужна фотография.")
        return CLEANUP

    cl = context.user_data["close"]["cleanup"]
    cl.append(file_id)
    if len(cl) < 4:
        await update.message.reply_text(f"Принял ✅ Фото {len(cl)}/4. Жду следующее.")
        return CLEANUP

    # ФИНАЛИЗАЦИЯ
    close_ctx = context.user_data["close"]
    point = close_ctx["point"]
    day = close_ctx["day"]
    session_id = close_ctx["session_id"]
    mode = close_ctx["mode"]
    cash_in = float(close_ctx["cash_in"])
    sales_cashless = float(close_ctx["sales_cashless"])
    sales_cash = float(close_ctx["sales_cash"])
    refunds = float(close_ctx["refunds"])
    total_sales = sales_cash + sales_cashless
    cash_in_box = cash_in + sales_cash

    # задачи по всей смене на точке (и для FULL, и для HALF2 при итоговом закрытии)
    tasks_all = load_tasks_for_today(point)
    done_ids = get_done_task_ids(day, point)
    missing = [t.task_name for t in tasks_all if t.task_id not in done_ids]

    note = ""
    if missing:
        note = "MISSING_TASKS"

    # лог close_log
    ts = now_tz().isoformat(timespec="seconds")
    cleanup = cl[:4]
    sheet_append(
        SHEET_CLOSE,
        [
            ts, day, point, session_id, mode,
            str(u.user_id), sanitize_for_sheets(u.name),
            str(cash_in), str(sales_cashless), str(sales_cash), str(refunds),
            str(total_sales), str(cash_in_box),
            close_ctx["receipt1"], close_ctx["receipt2"],
            cleanup[0], cleanup[1], cleanup[2], cleanup[3],
            note,
        ],
    )

    # закрыть сессию
    sess, _ = get_session(day, point)
    if sess and sess.session_id == session_id:
        sess.state = "CLOSED"
        if mode == "FULL":
            sess.user1_end = ts
        if mode == "HALF":
            role = close_ctx.get("role")
            if role == "HALF1":
                sess.user1_end = ts
            else:
                sess.user2_end = ts
        upsert_session(sess)

    # сообщение пользователю
    if missing:
        await update.message.reply_text(
            "Лично претензий к тебе нет, но косячек с тебя снял! Руководитель будет крайне не доволен!😌\n"
            "Задания на сегодня тобою не выполнены.\n\n"
            "Смена закрыта ✅\n\nВыбери точку:",
            reply_markup=after_approved_kb(),
        )
    else:
        await update.message.reply_text("Смена закрыта ✅\n\nВыбери точку:", reply_markup=after_approved_kb())

    # отчет в контроль (с цифрами)
    summary = (
        f"🔒 Закрытие смены\n"
        f"Точка: {point}\n"
        f"Сотрудник: {u.name} ({u.user_id})\n"
        f"Внесение {cash_in}\n"
        f"Наличные {sales_cash}\n"
        f"Безнал {sales_cashless}\n"
        f"Возвраты {refunds}\n"
        f"Итого за смену {total_sales} (наличные+безнал)\n"
        f"Наличные в кассе {cash_in_box} (внесение+наличные)\n"
        f"Время: {ts}"
    )
    await report_to_control(context, summary)

    # фото: 2 чека
    await report_photo_to_control(context, close_ctx["receipt1"], caption=f"🧾 Чек 1\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")
    await report_photo_to_control(context, close_ctx["receipt2"], caption=f"🧾 Чек 2\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")
    # фото: уборка 4
    for i, pid in enumerate(cleanup, start=1):
        await report_photo_to_control(context, pid, caption=f"🧹 Уборка {i}/4\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")

    if missing:
        await report_to_control(
            context,
            format_control(
                "⚠️ Косяк: задачи не выполнены к закрытию смены",
                u.name,
                u.user_id,
                point=point,
                details=["Не выполнены:"] + [f"• {x}" for x in missing[:30]],
            ),
        )

    # очистить контекст
    context.user_data.pop("close", None)
    return ConversationHandler.END


async def close_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop("close", None)
    await update.message.reply_text("Ок, отменил закрытие смены.", reply_markup=open_choice_kb())
    return ConversationHandler.END


# -------------------- REMINDERS --------------------


REMINDER_TEXT = "Дружище, ты же помнишь о задачах? Давай не будем подводить друг друга и закроем план! 🙂"
CLOSE_AVAILABLE_TEXT = "🔒 Кнопка «Закрыть смену» теперь доступна. Нажми её, чтобы закрыть смену."


async def reminders_job(context: ContextTypes.DEFAULT_TYPE):
    if not ENABLE_REMINDERS:
        return

    d = day_key()
    sessions = list_open_sessions()
    if not sessions:
        return

    # Пушим сотруднику актуальное меню с кнопкой закрытия в момент окончания смены.
    # (иначе у второго сотрудника «закрыть смену» не появится, если он принял смену раньше конца)
    for s in sessions:
        if s.day != d:
            continue
        point = normalize_point(s.point)
        if not can_close_now(point):
            continue
        notify_uid = None
        notify_role = None
        if s.mode == "FULL" and s.state == "OPEN_FULL" and s.user1_id:
            notify_uid = int(s.user1_id)
            notify_role = "FULL"
        elif s.mode == "HALF" and s.state == "OPEN2" and s.user2_id:
            notify_uid = int(s.user2_id)
            notify_role = "HALF2"
        if notify_uid is None:
            continue
        flag_key = f"close_notified:{s.session_id}:{notify_uid}"
        if context.bot_data.get(flag_key):
            continue
        context.bot_data[flag_key] = True
        try:
            await context.bot.send_message(
                chat_id=notify_uid,
                text=CLOSE_AVAILABLE_TEXT,
                reply_markup=shift_kb(notify_role, point),
            )
        except Exception as e:
            log.warning("Не смог отправить уведомление о закрытии %s: %s", notify_uid, e)

    for s in sessions:
        if s.day != d:
            continue
        point = normalize_point(s.point)
        if not in_work_hours(point):
            continue

        # кто сейчас отвечает за задачи
        targets: List[Tuple[int, str]] = []
        if s.mode == "FULL" and s.state == "OPEN_FULL" and s.user1_id:
            targets.append((int(s.user1_id), "FULL"))
        elif s.mode == "HALF":
            if s.state == "OPEN1" and s.user1_id:
                targets.append((int(s.user1_id), "HALF1"))
            elif s.state == "OPEN2" and s.user2_id:
                targets.append((int(s.user2_id), "HALF2"))
            else:
                continue

        tasks_all = load_tasks_for_today(point)
        if not tasks_all:
            continue

        done_ids = get_done_task_ids(d, point)
        for uid, role in targets:
            # определить задачи для роли
            if role == "FULL":
                tasks = tasks_all
            else:
                split_index = int(s.split_index or "0")
                tasks = tasks_all[:split_index] if role == "HALF1" else tasks_all[split_index:]

            remaining = [t for t in tasks if t.task_id not in done_ids]
            if not remaining:
                continue

            last_ts = last_task_action_ts(d, point, uid)
            if last_ts is None:
                # если не делал ничего и прошло >= idle от старта его смены
                start_ts_str = s.user1_start if role in ("FULL", "HALF1") else s.user2_start
                try:
                    start_ts = datetime.fromisoformat(start_ts_str)
                except Exception:
                    start_ts = now_tz()
                if now_tz() - start_ts < timedelta(minutes=REMINDER_IDLE_MINUTES):
                    continue
            else:
                if now_tz() - last_ts < timedelta(minutes=REMINDER_IDLE_MINUTES):
                    continue

            # throttling: не чаще чем раз в REMINDER_IDLE_MINUTES для (день/точка/сотрудник)
            flag = f"reminder_sent:{d}:{point}:{uid}"
            last = context.bot_data.get(flag)  # ISO timestamp
            if last:
                try:
                    last_dt = datetime.fromisoformat(last)
                    if now_tz() - last_dt < timedelta(minutes=REMINDER_IDLE_MINUTES):
                        continue
                except Exception:
                    pass
            context.bot_data[flag] = now_tz().isoformat(timespec="seconds")

            try:
                await context.bot.send_message(chat_id=uid, text=REMINDER_TEXT)
            except Exception as e:
                log.warning("Не смог отправить напоминание %s: %s", uid, e)


# -------------------- ERROR HANDLER --------------------


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Ошибка: %s", context.error)


# -------------------- HEALTH SERVER (polling mode) --------------------


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


# -------------------- APP BUILD --------------------


# -------------------- DAILY TOTALS (23:50) --------------------

def _to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        s = str(x).strip().replace(" ", "").replace(",", ".")
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _fmt_money(v: float) -> str:
    try:
        if abs(v - round(v)) < 1e-9:
            return str(int(round(v)))
        return f"{v:.2f}"
    except Exception:
        return "0"


def collect_daily_totals(day: str) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
    """Берём ПОСЛЕДНЕЕ закрытие на точке за день (по timestamp) и считаем итоги по точкам."""
    points = [normalize_point(p) for p in load_points()]
    # дефолт 0 по всем
    metrics: Dict[str, Dict[str, float]] = {
        p: {
            "cash_in": 0.0,
            "sales_cash": 0.0,
            "sales_cashless": 0.0,
            "refunds": 0.0,
            "total_sales": 0.0,
            "cash_in_box": 0.0,
        } for p in points
    }

    rows = sheet_get(SHEET_CLOSE)
    if not rows:
        return points, metrics

    start = 1 if (rows and is_header(rows[0], "timestamp")) else 0

    # last close per point
    best: Dict[str, Tuple[datetime, List[str]]] = {}
    for r in rows[start:]:
        if len(r) < 13:
            continue
        if (r[1] or "").strip() != day:
            continue
        p = normalize_point(r[2])
        if p not in metrics:
            # на случай, если в close_log точка есть, а в points её нет
            metrics[p] = {
                "cash_in": 0.0,
                "sales_cash": 0.0,
                "sales_cashless": 0.0,
                "refunds": 0.0,
                "total_sales": 0.0,
                "cash_in_box": 0.0,
            }
            points.append(p)
        ts_s = (r[0] or "").strip()
        try:
            ts = datetime.fromisoformat(ts_s)
        except Exception:
            # если вдруг формат поехал — считаем "самым старым"
            ts = datetime(1970, 1, 1)
        cur = best.get(p)
        if cur is None or ts > cur[0]:
            best[p] = (ts, r)

    # заполняем по лучшим строкам
    for p, (_ts, r) in best.items():
        metrics[p]["cash_in"] = _to_float(r[7])
        metrics[p]["sales_cashless"] = _to_float(r[8])
        metrics[p]["sales_cash"] = _to_float(r[9])
        metrics[p]["refunds"] = _to_float(r[10])
        metrics[p]["total_sales"] = _to_float(r[11])
        metrics[p]["cash_in_box"] = _to_float(r[12])

    return points, metrics


def build_totals_table_text(day: str, points: List[str], metrics: Dict[str, Dict[str, float]]) -> str:
    headers = ["", "69", "Арена", "Музей", "Сочнева", "Итого"]

    # порядок колонок ровно как в примере
    order = ["69 Параллель", "Арена", "Музей", "Сочнева"]
    cols = [p for p in order if p in metrics]  # только те, что есть
    # если есть необычные точки — добавим их в конец
    for p in points:
        if p not in cols:
            cols.append(p)

    def val(p: str, key: str) -> float:
        return float(metrics.get(p, {}).get(key, 0.0))

    rows = [
        ("Внесение", "cash_in"),
        ("Наличные", "sales_cash"),
        ("Безнал", "sales_cashless"),
        ("Возвраты", "refunds"),
        ("Итого за смену (наличные+безнал)", "total_sales"),
        ("Итого в кассе (внесение+наличные)", "cash_in_box"),
    ]

    # ширины
    col_names = [""] + cols + ["Итого"]
    col_w = []
    # первая колонка шире
    col_w.append(max(10, max(len(r[0]) for r in rows)))
    for p in cols:
        col_w.append(max(len(p), 9))
    col_w.append(9)

    def fmt_cell(s: str, w: int, right: bool = False) -> str:
        if right:
            return s.rjust(w)
        return s.ljust(w)

    # header line
    lines = []
    header_cells = [fmt_cell("", col_w[0])]
    for i, p in enumerate(cols, start=1):
        header_cells.append(fmt_cell(p, col_w[i], right=False))
    header_cells.append(fmt_cell("Итого", col_w[-1], right=False))
    lines.append(" | ".join(header_cells))

    # separator
    lines.append("-" * (sum(col_w) + 3 * (len(col_w)-1)))

    # data rows
    for label, key in rows:
        cells = [fmt_cell(label, col_w[0])]
        row_total = 0.0
        for i, p in enumerate(cols, start=1):
            v = val(p, key)
            row_total += v
            cells.append(fmt_cell(_fmt_money(v), col_w[i], right=True))
        cells.append(fmt_cell(_fmt_money(row_total), col_w[-1], right=True))
        lines.append(" | ".join(cells))

    return "\n".join(lines)



# -------------------- DAILY TOTALS (compact, mobile-friendly) --------------------

def _short_point_name(p: str) -> str:
    m = {
        "69 Параллель": "69",
        "Арена": "Ар",
        "Музей": "Муз",
        "Сочнева": "Соч",
    }
    if p in m:
        return m[p]
    s = (p or "").strip()
    if len(s) <= 6:
        return s or "?"
    return s[:6] + "…"


def _build_totals_table_compact(cols: List[str], metrics: Dict[str, Dict[str, float]]) -> str:
    rows = [
        ("Внес", "cash_in"),
        ("Нал", "sales_cash"),
        ("Безн", "sales_cashless"),
        ("Возв", "refunds"),
        ("Смена", "total_sales"),
        ("Касса", "cash_in_box"),
    ]

    def val(p: str, key: str) -> float:
        return float(metrics.get(p, {}).get(key, 0.0))

    # widths
    label_w = max(1, max(len(lbl) for lbl, _ in rows))
    col_ws: List[int] = []
    for p in cols:
        w = len(_short_point_name(p))
        for _, key in rows:
            w = max(w, len(_fmt_money(val(p, key))))
        col_ws.append(w)

    sigma_w = len("Σ")
    for _, key in rows:
        sigma_w = max(sigma_w, len(_fmt_money(sum(val(p, key) for p in cols))))

    def cell(s: str, w: int, right: bool = False) -> str:
        s = s or ""
        return s.rjust(w) if right else s.ljust(w)

    lines: List[str] = []
    header_cells = [cell("", label_w)] + [
        cell(_short_point_name(p), w, right=True) for p, w in zip(cols, col_ws)
    ] + [cell("Σ", sigma_w, right=True)]
    lines.append(" | ".join(header_cells))
    lines.append("-" * (sum([label_w] + col_ws + [sigma_w]) + 3 * (len(col_ws) + 1)))

    for lbl, key in rows:
        row_cells = [cell(lbl, label_w)]
        row_sum = 0.0
        for p, w in zip(cols, col_ws):
            v = val(p, key)
            row_sum += v
            row_cells.append(cell(_fmt_money(v), w, right=True))
        row_cells.append(cell(_fmt_money(row_sum), sigma_w, right=True))
        lines.append(" | ".join(row_cells))

    return "\n".join(lines)


def build_totals_messages_compact(day: str, points: List[str], metrics: Dict[str, Dict[str, float]]):
    order = ["69 Параллель", "Арена", "Музей", "Сочнева"]
    cols: List[str] = [p for p in order if p in metrics]
    for p in points:
        if p not in cols:
            cols.append(p)

    groups: List[List[str]] = []
    i = 0
    while i < len(cols):
        groups.append(cols[i:i + 2])
        i += 2

    tables = []
    for g in groups:
        title = " + ".join(_short_point_name(p) for p in g)
        tables.append((title, _build_totals_table_compact(g, metrics)))

    total_sales = sum(float(metrics.get(p, {}).get("total_sales", 0.0)) for p in cols)
    cash_in_box = sum(float(metrics.get(p, {}).get("cash_in_box", 0.0)) for p in cols)
    return tables, total_sales, cash_in_box


async def _send_pre_table(bot, chat_id: int, header: str, table_text: str):
    payload = html.escape(table_text)
    text = header + "\n<pre>" + payload + "</pre>"

    if len(text) <= 3900:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
        return

    # split by lines if too long
    lines = payload.split("\n")
    chunk: List[str] = []
    cur_len = len(header) + len("<pre></pre>") + 2

    for ln in lines:
        add_len = len(ln) + 1
        if chunk and cur_len + add_len > 3800:
            body = "\n".join(chunk)
            await bot.send_message(chat_id=chat_id, text=header + "\n<pre>" + body + "</pre>", parse_mode="HTML")
            chunk = []
            cur_len = len(header) + len("<pre></pre>") + 2
        chunk.append(ln)
        cur_len += add_len

    if chunk:
        body = "\n".join(chunk)
        await bot.send_message(chat_id=chat_id, text=header + "\n<pre>" + body + "</pre>", parse_mode="HTML")


async def daily_totals_job(context: ContextTypes.DEFAULT_TYPE):
    if not ENABLE_DAILY_TOTALS:
        return
    if CONTROL_GROUP_ID == 0:
        return

    d = day_key()
    points, metrics = collect_daily_totals(d)
    if not metrics:
        return

    try:
        tables, total_sales, cash_in_box = build_totals_messages_compact(d, points, metrics)
        for title, table in tables:
            await _send_pre_table(context.bot, CONTROL_GROUP_ID, f"📊 Итоги за {d} ({title})", table)
        await context.bot.send_message(
            chat_id=CONTROL_GROUP_ID,
            text=f"📊 Итоги за {d}: Смена={_fmt_money(total_sales)}  Касса={_fmt_money(cash_in_box)}",
        )
    except Exception as e:
        log.warning("Не смог отправить ежедневные итоги: %s", e)


async def cmd_totals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual daily totals in control group: /totals [вчера|yesterday|YYYY-MM-DD]"""
    if not ENABLE_DAILY_TOTALS:
        await update.effective_message.reply_text("Итоги отключены.")
        return

    chat_id = update.effective_chat.id if update.effective_chat else 0
    if CONTROL_GROUP_ID and chat_id != CONTROL_GROUP_ID:
        await update.effective_message.reply_text("Команда доступна только в группе контроля.")
        return

    d = day_key()
    if getattr(context, "args", None):
        arg = " ".join(context.args).strip()
        low = arg.lower()
        if low in ("yesterday", "вчера"):
            d = (now_tz() - timedelta(days=1)).date().isoformat()
        else:
            try:
                d = date.fromisoformat(arg).isoformat()
            except Exception:
                await update.effective_message.reply_text("Формат: /totals [вчера|yesterday|YYYY-MM-DD]")
                return

    points, metrics = collect_daily_totals(d)
    if not metrics:
        await update.effective_message.reply_text(f"Нет данных за {d}.")
        return

    tables, total_sales, cash_in_box = build_totals_messages_compact(d, points, metrics)
    for title, table in tables:
        await _send_pre_table(context.bot, chat_id, f"📊 Итоги за {d} ({title})", table)
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📊 Итоги за {d}: Смена={_fmt_money(total_sales)}  Касса={_fmt_money(cash_in_box)}",
    )


def build_app() -> Application:
    require_env()

    try:
        ensure_sheets()
    except HttpError as e:
        raise RuntimeError(
            "Не получилось подключиться к таблице.\n"
            "Проверь:\n"
            "1) SPREADSHEET_ID\n"
            "2) что сервис-аккаунт добавлен в «Поделиться» как Редактор\n"
            f"\nОшибка: {e}"
        ) from e

    app = Application.builder().token(BOT_TOKEN).build()

    # Registration conversation
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start_cmd)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_code)],
        },
        fallbacks=[],
    )
    app.add_handler(reg_conv)

    # Admin commands & buttons
    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^ADM\|"))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("totals", cmd_totals))
    app.add_handler(CommandHandler("unblock", cmd_unblock))
    app.add_handler(CommandHandler("pending", cmd_pending))

    # Employee callbacks
    app.add_handler(CallbackQueryHandler(choose_point_cb, pattern=r"^CHOOSE_POINT$"))
    app.add_handler(CallbackQueryHandler(point_pick_cb, pattern=r"^POINT\|\d+$"))
    app.add_handler(CallbackQueryHandler(back_to_point_cb, pattern=r"^BACK_TO_POINT$"))


    # Open FULL shift conversation (report -> showcase photo -> macarons photo)
    open_full_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(open_full_start_cb, pattern=r"^OPEN\|(FULL|HALF)$")],
        states={
            OPEN_FULL_REPORT: [MessageHandler(filters.TEXT & ~filters.COMMAND, open_full_report_text)],
            OPEN_FULL_SHOWCASE: [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, open_full_showcase_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, open_full_need_showcase),
            ],
            OPEN_FULL_MACARONS: [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, open_full_macarons_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, open_full_need_macarons),
            ],
        },
        fallbacks=[CommandHandler("start", start_cmd)],
        allow_reentry=True,
    )
    app.add_handler(open_full_conv)
    app.add_handler(CallbackQueryHandler(open_cb, pattern=r"^OPEN\|"))

    app.add_handler(CallbackQueryHandler(plan_cb, pattern=r"^PLAN$"))
    app.add_handler(CallbackQueryHandler(mark_cb, pattern=r"^MARK$"))
    app.add_handler(CallbackQueryHandler(task_pick_cb, pattern=r"^TASK\|\d+$"))
    app.add_handler(CallbackQueryHandler(skip_task_photo2_cb, pattern=r"^SKIP_TASK_PHOTO2$"))

    app.add_handler(CallbackQueryHandler(help_cb, pattern=r"^HELP$"))
    app.add_handler(CallbackQueryHandler(help_send_cb, pattern=r"^HELP_SEND$"))
    app.add_handler(CallbackQueryHandler(help_cancel_cb, pattern=r"^HELP_CANCEL$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, help_text_message), group=1)

    app.add_handler(CallbackQueryHandler(transfer_cb, pattern=r"^TRANSFER$"))
    app.add_handler(CallbackQueryHandler(pick_user2_cb, pattern=r"^U2\|\d+$"))
    app.add_handler(CallbackQueryHandler(accept_shift_cb, pattern=r"^ACCEPT\|"))

    app.add_handler(CallbackQueryHandler(back_main_cb, pattern=r"^BACK_MAIN$"))
    app.add_handler(CallbackQueryHandler(back_shift_cb, pattern=r"^BACK_SHIFT$"))

    # Close shift conversation
    close_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(close_start_cb, pattern=r"^CLOSE$")],
        states={
            CASH_IN: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_cash_in)],
            SALES_CASHLESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_sales_cashless)],
            SALES_CASH: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_sales_cash)],
            REFUNDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_refunds)],
            RECEIPT1: [MessageHandler(filters.PHOTO | filters.Document.IMAGE, close_receipt1)],
            RECEIPT2: [MessageHandler(filters.PHOTO | filters.Document.IMAGE, close_receipt2)],
            CLEANUP: [MessageHandler(filters.PHOTO | filters.Document.IMAGE, close_cleanup)],
        },
        fallbacks=[CommandHandler("cancel", close_cancel)],
        allow_reentry=True,
    )
    app.add_handler(close_conv)

    # Photo handler (open full / task / help)
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, photo_message))

    app.add_error_handler(error_handler)

    # Reminders
    if ENABLE_REMINDERS and app.job_queue:
        interval = max(1, REMINDER_CHECK_MINUTES) * 60
        app.job_queue.run_repeating(reminders_job, interval=interval, first=interval, name="task_reminders")
        log.info("Reminders enabled: check every %s minutes, idle=%s minutes", REMINDER_CHECK_MINUTES, REMINDER_IDLE_MINUTES)
    else:
        log.info("Reminders disabled or JobQueue not available")

    # Daily totals at 23:50 (local TIME_ZONE)
    if ENABLE_DAILY_TOTALS and app.job_queue:
        try:
            t = time(DAILY_TOTALS_HOUR, DAILY_TOTALS_MINUTE, tzinfo=_tz)
            app.job_queue.run_daily(daily_totals_job, time=t, name="daily_totals_2350")
            log.info("Daily totals enabled: %02d:%02d (%s)", DAILY_TOTALS_HOUR, DAILY_TOTALS_MINUTE, TIME_ZONE)
        except Exception as e:
            log.warning("Daily totals schedule failed: %s", e)
    else:
        log.info("Daily totals disabled or JobQueue not available")

    return app


def main():
    tg_app = build_app()

    log.info(
        "BOOT: WEBHOOK_MODE=%s BASE=%s PATH=%s PORT=%s TZ=%s",
        WEBHOOK_MODE,
        WEBHOOK_BASE_URL,
        WEBHOOK_PATH,
        os.getenv("PORT"),
        TIME_ZONE,
    )

    if WEBHOOK_MODE:
        if not WEBHOOK_BASE_URL:
            raise RuntimeError("WEBHOOK_BASE_URL is empty (set it in Render Environment)")

        port = int(os.getenv("PORT", "10000"))
        path = WEBHOOK_PATH

        async def health(_request: web.Request) -> web.Response:
            return web.Response(text="OK")

        async def webhook_handler(request: web.Request) -> web.Response:
            try:
                data = await request.json()
            except Exception:
                return web.Response(status=400, text="bad json")

            try:
                update = Update.de_json(data, tg_app.bot)
                if tg_app.update_queue.qsize() >= MAX_WEBHOOK_QUEUE:
                    log.warning("Webhook queue overflow (qsize=%s >= %s) — dropping update", tg_app.update_queue.qsize(), MAX_WEBHOOK_QUEUE)
                else:
                    await tg_app.update_queue.put(update)
            except Exception as e:
                log.exception("Webhook update processing error: %s", e)

            return web.Response(text="OK")

        async def on_startup(_app: web.Application):
            await tg_app.initialize()
            await tg_app.start()

            url = f"{WEBHOOK_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
            await tg_app.bot.set_webhook(
                url=url,
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
            )
            log.info("Webhook mode ON: %s  port=%s", url, port)

        async def on_cleanup(_app: web.Application):
            # Важно: не дергать stop/shutdown, чтобы не удалять webhook на Render
            return

        aio = web.Application()
        aio.router.add_get("/", health)
        aio.router.add_get("/health", health)
        aio.router.add_get("/healthz", health)
        aio.router.add_post(f"/{path}", webhook_handler)
        aio.on_startup.append(on_startup)
        aio.on_cleanup.append(on_cleanup)

        web.run_app(aio, host="0.0.0.0", port=port)
    else:
        log.info("Polling mode ON")
        start_health_server()
        tg_app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
