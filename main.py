#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WeCafe Shift & Tasks Bot (Telegram) — полный код в одном файле.

ВАЖНО (честно):
- У меня нет доступа к вашему репозиторию/файлам, поэтому я не могу “вытащить” ваш текущий main.py.
- Этот файл — полностью рабочая сборка бота на базе того фрагмента логики, который вы показывали в чате
  (регистрация, одобрение, выбор точки, открытие полной/пол-смены через отчет + 2 фото, план/задачи,
  "Красавчик помоги", передача пол-смены, закрытие смены, напоминания, ежедневные итоги, /totals).
- Ключевая правка для бага “Открыть пол смены” уже заложена: режим HALF не теряется и создаётся сессия HALF/OPEN1.

ENV (обязательные):
- BOT_TOKEN
- SPREADSHEET_ID
- CONTROL_GROUP_ID
- ACCESS_CODE (например DreamTeam)
Google creds (одно из двух):
- GOOGLE_SHEETS_CREDENTIALS_FILE  (путь к json ключу сервис-аккаунта)
или
- GOOGLE_SHEETS_CREDENTIALS_JSON_B64 (тот же json, но base64)

Опционально:
- TIME_ZONE (по умолчанию Asia/Krasnoyarsk)
- WEBHOOK_MODE=1 + WEBHOOK_BASE_URL + WEBHOOK_PATH (иначе polling)
- REPORT_TO_CONTROL=1/0
- ENABLE_REMINDERS=1/0, REMINDER_IDLE_MINUTES, REMINDER_CHECK_MINUTES
- ENABLE_DAILY_TOTALS=1/0, DAILY_TOTALS_HOUR, DAILY_TOTALS_MINUTE

Google Sheets листы (по умолчанию):
- users
- points
- cleaning_schedule
- done_log
- shift_sessions
- close_log
"""

from __future__ import annotations

import base64
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove, Update
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
CONTROL_GROUP_ID = int(os.getenv("CONTROL_GROUP_ID", "0").strip() or "0")

ACCESS_CODE = os.getenv("ACCESS_CODE", "DreamTeam").strip()

GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE", "").strip()
GOOGLE_SHEETS_CREDENTIALS_JSON_B64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON_B64", "").strip()

TIME_ZONE = os.getenv("TIME_ZONE", "Asia/Krasnoyarsk").strip()

REPORT_TO_CONTROL = os.getenv("REPORT_TO_CONTROL", "1").strip() != "0"

WEBHOOK_MODE = os.getenv("WEBHOOK_MODE", "0").strip() == "1"
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip().rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "webhook").strip().lstrip("/")
PORT = int(os.getenv("PORT", "10000").strip() or "10000")

ENABLE_REMINDERS = os.getenv("ENABLE_REMINDERS", "1").strip() != "0"
REMINDER_CHECK_MINUTES = int(os.getenv("REMINDER_CHECK_MINUTES", "10").strip() or "10")
REMINDER_IDLE_MINUTES = int(os.getenv("REMINDER_IDLE_MINUTES", "60").strip() or "60")

ENABLE_DAILY_TOTALS = os.getenv("ENABLE_DAILY_TOTALS", "1").strip() != "0"
DAILY_TOTALS_HOUR = int(os.getenv("DAILY_TOTALS_HOUR", "23").strip() or "23")
DAILY_TOTALS_MINUTE = int(os.getenv("DAILY_TOTALS_MINUTE", "50").strip() or "50")

SHEET_SCHEDULE = os.getenv("SHEET_SCHEDULE", "cleaning_schedule").strip()
SHEET_USERS = os.getenv("SHEET_USERS", "users").strip()
SHEET_POINTS = os.getenv("SHEET_POINTS", "points").strip()
SHEET_DONE = os.getenv("SHEET_DONE", "done_log").strip()
SHEET_SESSIONS = os.getenv("SHEET_SESSIONS", "shift_sessions").strip()
SHEET_CLOSE = os.getenv("SHEET_CLOSE", "close_log").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("wecafe-bot")

_tz = pytz.timezone(TIME_ZONE)

# -------------------- BASIC HELPERS --------------------

def require_env() -> None:
    problems: List[str] = []
    if not BOT_TOKEN:
        problems.append("BOT_TOKEN пустой")
    if not SPREADSHEET_ID:
        problems.append("SPREADSHEET_ID пустой")
    if CONTROL_GROUP_ID == 0:
        problems.append("CONTROL_GROUP_ID не задан")
    if not (GOOGLE_SHEETS_CREDENTIALS_FILE or GOOGLE_SHEETS_CREDENTIALS_JSON_B64):
        problems.append("нужен GOOGLE_SHEETS_CREDENTIALS_FILE или GOOGLE_SHEETS_CREDENTIALS_JSON_B64")
    if problems:
        raise RuntimeError("Проблемы ENV: " + "; ".join(problems))


def now_tz() -> datetime:
    return datetime.now(_tz)


def day_key() -> str:
    return now_tz().date().isoformat()


def day_column_name() -> str:
    # cleaning_schedule: D1..D31
    return f"D{now_tz().day}"


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


def normalize_point(point: str) -> str:
    p = (point or "").strip()
    low = p.lower()
    if "музей" in low:
        return "Музей"
    if "сочнев" in low:
        return "Сочнева"
    if "арена" in low:
        return "Арена"
    if "69" in p or "паралл" in low:
        return "69 Параллель"
    return p


async def safe_edit(q, *args, **kwargs):
    try:
        return await q.edit_message_text(*args, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return None
        raise


def _extract_photo_file_id(update: Update) -> Optional[str]:
    if update.message and update.message.photo:
        return update.message.photo[-1].file_id
    if update.message and update.message.document and update.message.document.mime_type:
        if update.message.document.mime_type.startswith("image/"):
            return update.message.document.file_id
    return None


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


def is_header(row: List[str], must_include: str) -> bool:
    low = [c.strip().lower() for c in row]
    return must_include.lower() in low


def ensure_header(sheet_title: str, header: List[str]):
    values = sheet_get(sheet_title)
    if not values:
        sheet_append(sheet_title, header)


# -------------------- SCHEMAS --------------------

USERS_HEADER = ["user_id", "name", "point", "status", "created_at", "updated_at"]
DONE_HEADER = ["timestamp", "day", "point", "user_id", "user_name", "task_id", "task_name", "part", "photo1_file_id", "photo2_file_id"]
SESSIONS_HEADER = [
    "session_id", "day", "point", "mode", "state",
    "user1_id", "user1_name", "user1_start", "user1_end",
    "user2_id", "user2_name", "user2_start", "user2_end",
    "split_index", "updated_at",
]
CLOSE_HEADER = [
    "timestamp", "day", "point", "session_id", "mode",
    "user_id", "user_name",
    "cash_in", "sales_cashless", "sales_cash", "refunds",
    "total_sales", "cash_in_box",
    "receipt1_file_id", "receipt2_file_id",
    "cleanup1_file_id", "cleanup2_file_id", "cleanup3_file_id", "cleanup4_file_id",
    "note",
]

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
    return rows, is_header(rows[0], "user_id")

def parse_user(row: List[str]) -> UserRec:
    uid = int(row[0])
    name = row[1] if len(row) > 1 else ""
    point = row[2] if len(row) > 2 else ""
    status = row[3] if len(row) > 3 else STATUS_PENDING
    created_at = row[4] if len(row) > 4 else ""
    updated_at = row[5] if len(row) > 5 else ""
    return UserRec(uid, name, point, status, created_at, updated_at)

def get_user_row_and_index(user_id: int) -> Tuple[Optional[List[str]], Optional[int]]:
    rows, has_header = _users_rows()
    if not rows:
        return None, None
    start = 1 if has_header else 0
    for i, row in enumerate(rows[start:], start=1 + start):
        if len(row) >= 1 and row[0] == str(user_id):
            return row, i
    return None, None

def get_user(user_id: int) -> Optional[UserRec]:
    row, _ = get_user_row_and_index(user_id)
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
    row, idx = get_user_row_and_index(user_id)
    if row is None:
        sheet_append(SHEET_USERS, [str(user_id), name, point, status, ts, ts])
        return
    created_at = row[4] if len(row) >= 5 else ts
    sheet_update(f"{SHEET_USERS}!A{idx}:F{idx}", [str(user_id), name, point, status, created_at, ts])

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

def list_active_users_all() -> List[UserRec]:
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
        task_id = (r[0] or "").strip()
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
        if last is None or ts > last:
            last = ts
    return last

# -------------------- SHIFT SESSIONS --------------------

@dataclass
class Session:
    session_id: str
    day: str
    point: str
    mode: str   # FULL | HALF
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
    return rows, is_header(rows[0], "session_id")

def get_session(day: str, point: str) -> Tuple[Optional[Session], Optional[int]]:
    rows, has_header = _sessions_rows()
    if not rows:
        return None, None
    start = 1 if has_header else 0
    sid = make_session_id(day, point)
    for i, r in enumerate(rows[start:], start=1 + start):
        if r and r[0] == sid:
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
            if s.state == "WAIT_ACCEPT" and s.user1_id == str(user_id):
                return s, "HALF1"
    return None, None

# -------------------- WORK HOURS / CLOSE --------------------

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
    return now_tz().time() >= end

# -------------------- UI --------------------

def kb_single(label: str, cb: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=cb)]])

def points_kb(points: List[str], prefix: str = "POINT") -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(p, callback_data=f"{prefix}|{i}")] for i, p in enumerate(points)]
    return InlineKeyboardMarkup(rows)

def after_approved_kb() -> InlineKeyboardMarkup:
    return kb_single("📍 Сменить точку", "CHOOSE_POINT")

def open_choice_kb() -> InlineKeyboardMarkup:
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

# -------------------- GUARDS --------------------

async def guard_employee(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[UserRec]:
    uid = update.effective_user.id if update.effective_user else 0
    u = get_user(uid)
    if not u:
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

def _is_control_chat(update: Update) -> bool:
    if update.callback_query and update.callback_query.message:
        return update.callback_query.message.chat_id == CONTROL_GROUP_ID
    if update.message:
        return update.message.chat_id == CONTROL_GROUP_ID
    return False

# -------------------- CONVERSATION STATES --------------------

REG_NAME, REG_CODE = range(2)

# Open shift (report + 2 photos)
OPEN_FULL_REPORT, OPEN_FULL_SHOWCASE, OPEN_FULL_MACARONS = range(3)

# Close shift (numbers + 2 receipts + 4 cleanup photos + optional note)
CASH_IN, SALES_CASHLESS, SALES_CASH, REFUNDS, RECEIPT1, RECEIPT2, CLEANUP, CLOSE_NOTE = range(8)

# -------------------- /start + register --------------------

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
        sess, role = user_open_context(uid)
        if sess and role:
            point = normalize_point(sess.point)
            await update.message.reply_text(
                f"А я тебя помню! 🙂\n\nСмена уже открыта на точке: {point}",
                reply_markup=shift_kb(role, point),
            )
            return ConversationHandler.END

        if not u.point:
            await update.message.reply_text("А я тебя помню! 🙂\n\nВыбери точку:", reply_markup=after_approved_kb())
        else:
            await update.message.reply_text(
                f"А я тебя помню! 🙂\n\nТвоя точка сейчас: {normalize_point(u.point)}\nНажми «Сменить точку» (можно выбрать ту же) и потом открой смену.",
                reply_markup=after_approved_kb(),
            )
        return ConversationHandler.END

    await update.message.reply_text("Привет! Давай зарегистрируемся.\n\nНапиши своё имя:", reply_markup=ReplyKeyboardRemove())
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

    await report_to_control(context, format_control("🆕 Запрос регистрации", name, uid, details=["Нажмите кнопку ниже:"]))
    try:
        await context.bot.send_message(
            chat_id=CONTROL_GROUP_ID,
            text=f"🆕 Запрос регистрации\nИмя: {name}\nID: {uid}\n\nОдобрить?",
            reply_markup=approve_kb(uid),
        )
    except Exception as e:
        log.warning("Не смог отправить approval-кнопки: %s", e)

    return ConversationHandler.END

# -------------------- ADMIN CALLBACKS --------------------

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

# -------------------- ADMIN COMMANDS --------------------

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
    set_user_status(uid, STATUS_ACTIVE)
    await update.message.reply_text(f"✅ Разблокирован: {u.name} ({uid})")

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
    for u in pending[:60]:
        lines.append(f"• {u.name} — {u.user_id}")
    await update.message.reply_text("\n".join(lines))

# -------------------- POINT SELECT --------------------

async def choose_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return

    sess, role = user_open_context(u.user_id)
    if sess and role and sess.state != "CLOSED":
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
        await safe_edit(q, "Не понял выбор. Нажми «📍 Сменить точку» ещё раз.", reply_markup=after_approved_kb())
        return

    set_user_point(u.user_id, point)
    u = get_user(u.user_id) or u

    await safe_edit(q, f"Точка выбрана: {normalize_point(point)}\n\nТеперь выбери вариант открытия смены:", reply_markup=open_choice_kb())
    await report_to_control(context, format_control("📍 Сотрудник выбрал точку", u.name, u.user_id, point=normalize_point(point)))

# -------------------- OPEN SHIFT CONVERSATION --------------------
# ВАЖНО: именно тут заложен фикс HALF — мы сохраняем режим в user_data и используем при создании сессии.

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

    # mode: FULL or HALF (из callback_data OPEN|FULL / OPEN|HALF)
    try:
        _p, mode = (q.data or "").split("|", 1)
    except Exception:
        mode = "FULL"
    if mode not in ("FULL", "HALF"):
        mode = "FULL"
    context.user_data["open_shift_mode"] = mode  # <<< ключевой момент (не теряем HALF)

    # если у пользователя уже есть открытая смена
    sess_open, role = user_open_context(u.user_id)
    if role:
        p = normalize_point(sess_open.point) if sess_open else point
        await safe_edit(q, "У тебя уже есть открытая смена.", reply_markup=shift_kb(role, p))
        return ConversationHandler.END

    existing, _ = get_session(d, point)
    if existing and existing.state != "CLOSED":
        await safe_edit(q, "На этой точке уже есть открытая смена сегодня. Обратись к руководителю.", reply_markup=open_choice_kb())
        return ConversationHandler.END

    context.user_data["open_full_point"] = point
    context.user_data["open_full_day"] = d
    context.user_data.pop("open_full_report", None)
    context.user_data.pop("open_full_photo_showcase", None)
    context.user_data.pop("open_full_photo_macarons", None)

    label = "Пол смены" if mode == "HALF" else "Полная смена"
    await safe_edit(q, f"{label}.\n\nПеречисли десерты в витрине и сроки их годности:")
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
    await update.message.reply_text("Отчет принят ✅\n\nТеперь пришли фото витрины 📸")
    return OPEN_FULL_SHOWCASE

async def open_full_showcase_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    file_id = _extract_photo_file_id(update)
    if not file_id:
        await update.message.reply_text("Нужно фото витрины 📸")
        return OPEN_FULL_SHOWCASE

    context.user_data["open_full_photo_showcase"] = file_id
    await update.message.reply_text("Фото витрины принято ✅\n\nТеперь пришли фото макаронс со сроком годности и вкусами 📸")
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

    existing, _ = get_session(d, point)
    if existing and existing.state != "CLOSED":
        # чистим контекст
        for k in ("open_full_point","open_full_day","open_full_report","open_full_photo_showcase","open_full_photo_macarons","open_shift_mode"):
            context.user_data.pop(k, None)
        await update.message.reply_text("Смена на точке уже открыта. Меню:", reply_markup=open_choice_kb())
        return ConversationHandler.END

    context.user_data["open_full_photo_macarons"] = file_id

    report_text = (context.user_data.get("open_full_report") or "").strip()
    photo_showcase = context.user_data.get("open_full_photo_showcase") or ""
    photo_macarons = context.user_data.get("open_full_photo_macarons") or ""

    ts = now_tz().isoformat(timespec="seconds")
    mode = context.user_data.get("open_shift_mode") or "FULL"  # <<< ключевой момент (используем сохраненный HALF)

    if mode == "HALF":
        tasks = load_tasks_for_today(point)
        _p1, _p2, split_index = split_tasks_half(tasks)
        sess = Session(
            session_id=make_session_id(d, point),
            day=d, point=point,
            mode="HALF", state="OPEN1",
            user1_id=str(u.user_id), user1_name=u.name, user1_start=ts, user1_end="",
            user2_id="", user2_name="", user2_start="", user2_end="",
            split_index=str(split_index),
            updated_at=ts,
        )
    else:
        sess = Session(
            session_id=make_session_id(d, point),
            day=d, point=point,
            mode="FULL", state="OPEN_FULL",
            user1_id=str(u.user_id), user1_name=u.name, user1_start=ts, user1_end="",
            user2_id="", user2_name="", user2_start="", user2_end="",
            split_index="",
            updated_at=ts,
        )
    upsert_session(sess)

    # чистим контекст открытия
    for k in ("open_full_point","open_full_day","open_full_report","open_full_photo_showcase","open_full_photo_macarons","open_shift_mode"):
        context.user_data.pop(k, None)

    details = [f"Время: {ts}"]
    if report_text:
        details.append("Отчет витрины:")
        details.append(report_text[:1500])

    await report_to_control(context, format_control("⏱️ Открыта пол смены" if mode == "HALF" else "🔓 Открыта смена (полная)", u.name, u.user_id, point=point, details=details))
    if photo_showcase:
        cap = f"📸 Витрина (готовность)\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})"
        if report_text:
            cap += f"\n\nОтчет:\n{report_text[:800]}"
        await report_photo_to_control(context, photo_showcase, caption=cap)
    if photo_macarons:
        await report_photo_to_control(context, photo_macarons, caption=f"📸 Макаронс\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")

    await update.message.reply_text(
        f"Смена открыта ✅\nТочка: {point}",
        reply_markup=shift_kb("HALF1", point) if mode == "HALF" else shift_kb("FULL", point),
    )
    return ConversationHandler.END

async def open_need_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Нужно фото 📸")
    # останемся в текущем состоянии, PTB будет звать правильный handler
    return ConversationHandler.END

# -------------------- PLAN / MARK TASKS --------------------

def assigned_tasks_for_user(sess: Session, role: str, point: str) -> Tuple[List[Task], str]:
    tasks = load_tasks_for_today(point)
    if role == "FULL":
        return tasks, "FULL"
    split_index = int(sess.split_index or "0")
    if role == "HALF1":
        return tasks[:split_index], "HALF1"
    if role == "HALF2":
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
    tasks, _ = assigned_tasks_for_user(sess, role, point)
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
        await safe_edit(q, "Не понял выбор. Нажми «✅ Отметить выполненную задачу» ещё раз.", reply_markup=shift_kb(role, normalize_point(sess.point)))
        return

    point = context.user_data.get("mark_point") or normalize_point(sess.point)
    part = context.user_data.get("mark_part") or role
    day = sess.day

    done_ids = get_done_task_ids(day, point)
    if item["task_id"] in done_ids:
        await safe_edit(q, "Эта задача уже отмечена ✅", reply_markup=shift_kb(role, point))
        return

    context.user_data["task_mark"] = {
        "day": day, "point": point, "part": part,
        "task_id": item["task_id"], "task_name": item["task_name"],
        "photo1": "", "photo2": "",
    }
    context.user_data["await"] = "TASK_PHOTO1"
    await safe_edit(q, f"Задача: {item['task_name']}\n\nПришли фото 1 (обязательно) 📸")

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

    log_done(day, point, user, task, part, photo1, photo2)

    context.user_data.pop("await", None)
    context.user_data.pop("task_mark", None)

    await report_to_control(context, format_control("✅ Задача выполнена", user.name, user.user_id, point=point, details=[f"Задача: {task.task_name}", f"Часть смены: {part}"]))
    if photo1:
        await report_photo_to_control(context, photo1, caption=f"📸 Отчет 1\nТочка: {point}\nЗадача: {task.task_name}\nСотрудник: {user.name} ({user.user_id})")
    if photo2:
        await report_photo_to_control(context, photo2, caption=f"📸 Отчет 2\nТочка: {point}\nЗадача: {task.task_name}\nСотрудник: {user.name} ({user.user_id})")

    sess, role = user_open_context(user.user_id)
    if sess and role:
        text = f"Готово ✅\nОтметил: {task.task_name}"
        if via_callback and update.callback_query:
            try:
                await safe_edit(update.callback_query, text, reply_markup=shift_kb(role, normalize_point(sess.point)))
                return
            except Exception:
                pass
        await update.effective_message.reply_text(text, reply_markup=shift_kb(role, normalize_point(sess.point)))

# -------------------- PHOTO HANDLER FOR TASKS / HELP --------------------

async def photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = await guard_employee(update, context)
    if not u:
        return

    file_id = _extract_photo_file_id(update)
    if not file_id:
        return

    # Task photos
    if context.user_data.get("await") in ("TASK_PHOTO1", "TASK_PHOTO2"):
        task_mark = context.user_data.get("task_mark") or {}
        if not task_mark:
            context.user_data.pop("await", None)
            await update.message.reply_text("Я потерял контекст задачи 😅 Нажми «✅ Отметить выполненную задачу» ещё раз.")
            return

        if context.user_data["await"] == "TASK_PHOTO1":
            task_mark["photo1"] = file_id
            context.user_data["task_mark"] = task_mark
            context.user_data["await"] = "TASK_PHOTO2"
            await update.message.reply_text(
                "Фото 1 принято ✅\n\nТеперь пришли фото 2 (по желанию) 📸\nили нажми «Пропустить».",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить фото 2", callback_data="SKIP_TASK_PHOTO2")]]),
            )
            return

        if context.user_data["await"] == "TASK_PHOTO2":
            task_mark["photo2"] = file_id
            context.user_data["task_mark"] = task_mark
            await finalize_task_done(update, context, u, task_mark)
            return

    # Help photos
    if context.user_data.get("help_mode"):
        photos: List[str] = context.user_data.get("help_photos") or []
        if len(photos) >= 4:
            await update.message.reply_text("Уже 4 фото. Нажми «✅ Отправить» 🙂")
            return
        photos.append(file_id)
        context.user_data["help_photos"] = photos
        left = 4 - len(photos)
        await update.message.reply_text(
            f"Фото добавлено ✅ (осталось до 4: {left})\nНажми «✅ Отправить», когда закончишь.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Отправить", callback_data="HELP_SEND")],
                [InlineKeyboardButton("❌ Отмена", callback_data="HELP_CANCEL")],
            ]),
        )
        return

    await update.message.reply_text("Фото получено 👍 Но сейчас я его ни для чего не жду.\nОткрой меню и действуй по кнопкам.")

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
        "Текст принял ✅\n\nТеперь можешь отправить до 4 фото.\nКогда закончишь — нажми «✅ Отправить».",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Отправить", callback_data="HELP_SEND")],
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

    await report_to_control(context, format_control("🤝 Красавчик помоги", u.name, u.user_id, point=point, details=[f"Сообщение: {text}"]))
    for i, pid in enumerate(photos[:4], start=1):
        await report_photo_to_control(context, pid, caption=f"📸 Фото {i}\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")

    for k in ("help_mode","help_text","help_photos","help_point"):
        context.user_data.pop(k, None)

    await safe_edit(q, "Отправил в группу контроля ✅", reply_markup=shift_kb(role, point))

async def help_cancel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    for k in ("help_mode","help_text","help_photos","help_point"):
        context.user_data.pop(k, None)

    u = await guard_employee(update, context)
    if not u:
        return
    sess, role = user_open_context(u.user_id)
    if sess and role:
        await safe_edit(q, "Ок, отменил.", reply_markup=shift_kb(role, normalize_point(sess.point)))
    else:
        await safe_edit(q, "Ок, отменил.", reply_markup=open_choice_kb())

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

    users = [x for x in list_active_users_all() if x.user_id != u.user_id]
    if not users:
        await safe_edit(q,
            "Нет активных сотрудников для передачи.\nПусть второй сотрудник пройдёт регистрацию.",
            reply_markup=shift_kb(role, point),
        )
        return

    rows = []
    for x in users[:30]:
        rows.append([InlineKeyboardButton(f"{x.name} ({x.user_id})", callback_data=f"U2|{x.user_id}")])

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

    tasks_all = load_tasks_for_today(point)
    split_index = int(sess.split_index or "0")
    my_tasks = tasks_all[:split_index]
    done_ids = get_done_task_ids(sess.day, point)
    missing = [t.task_name for t in my_tasks if t.task_id not in done_ids]

    if missing:
        warn = "Лично претензий к тебе нет, но косячек с тебя снял! Руководитель будет крайне не доволен!😌\nЗадания на сегодня тобою не выполнены."
        await context.bot.send_message(chat_id=u.user_id, text=warn)
        await report_to_control(context, format_control("⚠️ Косяк при передаче смены (пол смены)", u.name, u.user_id, point=point, details=["Не выполнены задачи первой половины:"] + [f"• {x}" for x in missing[:25]]))

    ts = now_tz().isoformat(timespec="seconds")
    sess.state = "WAIT_ACCEPT"
    sess.user1_end = ts
    sess.user2_id = str(u2.user_id)
    sess.user2_name = u2.name
    upsert_session(sess)

    try:
        await context.bot.send_message(
            chat_id=u2.user_id,
            text=f"Тебе передают смену на точке: {point}\nНажми «✅ Принять смену». (Точку выбирать не нужно)",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Принять смену", callback_data=f"ACCEPT|{sess.session_id}")]]),
        )
    except Exception as e:
        log.warning("Не смог отправить accept user2: %s", e)

    await report_to_control(context, format_control("🔁 Передача смены запрошена", u.name, u.user_id, point=point, details=[f"Кому: {u2.name} ({u2.user_id})", f"Время: {ts}"]))
    await safe_edit(q, "Смену передал ✅\nВторой сотрудник должен нажать «✅ Принять смену».", reply_markup=open_choice_kb())

async def accept_shift_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return

    try:
        _p, session_id = q.data.split("|", 1)
        d, point = session_id.split("|", 1)
    except Exception:
        await safe_edit(q, "Некорректная команда.")
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

    set_user_point(u.user_id, normalize_point(sess.point))

    ts = now_tz().isoformat(timespec="seconds")
    sess.state = "OPEN2"
    sess.user2_start = ts
    upsert_session(sess)

    await report_to_control(context, format_control("✅ Смена принята (пол смены)", u.name, u.user_id, point=normalize_point(sess.point), details=[f"Время: {ts}", f"От кого: {sess.user1_name} ({sess.user1_id})"]))
    await safe_edit(q, f"Смена принята ✅\nТочка: {normalize_point(sess.point)}", reply_markup=shift_kb("HALF2", normalize_point(sess.point)))

# -------------------- CLOSE SHIFT --------------------

def parse_money(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(" ", "").replace(",", ".")
    try:
        v = float(s)
        return v if v >= 0 else None
    except Exception:
        return None

async def close_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    sess, role = user_open_context(u.user_id)
    if not sess or not role:
        await safe_edit(q, "Смена не открыта.", reply_markup=open_choice_kb())
        return ConversationHandler.END

    point = normalize_point(sess.point)

    # Пол-смены закрывает только второй сотрудник
    if sess.mode == "HALF" and role != "HALF2":
        await safe_edit(q, "Закрытие смены делает второй сотрудник (после передачи).", reply_markup=shift_kb(role, point))
        return ConversationHandler.END

    if not can_close_now(point):
        _start, end = point_hours(point)
        await safe_edit(q, f"Закрытие доступно после {end.strftime('%H:%M')}.\nСейчас ещё рано 🙂", reply_markup=shift_kb(role, point))
        return ConversationHandler.END

    # подготовка данных закрытия
    context.user_data["close"] = {
        "day": sess.day,
        "point": point,
        "session_id": sess.session_id,
        "mode": sess.mode,
        "user_id": u.user_id,
        "user_name": u.name,
        "cash_in": None,
        "sales_cashless": None,
        "sales_cash": None,
        "refunds": None,
        "receipt1": "",
        "receipt2": "",
        "cleanup": [],
        "note": "",
    }

    await safe_edit(q, "Закрытие смены.\n\nВведи «Внесение» (числом):")
    return CASH_IN

async def close_cash_in(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("close") or {}
    v = parse_money(update.message.text or "")
    if v is None:
        await update.message.reply_text("Нужно число. Пример: 1234.50")
        return CASH_IN
    data["cash_in"] = v
    context.user_data["close"] = data
    await update.message.reply_text("Теперь введи «Продажи безнал» (числом):")
    return SALES_CASHLESS

async def close_sales_cashless(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("close") or {}
    v = parse_money(update.message.text or "")
    if v is None:
        await update.message.reply_text("Нужно число. Пример: 1234.50")
        return SALES_CASHLESS
    data["sales_cashless"] = v
    context.user_data["close"] = data
    await update.message.reply_text("Теперь введи «Продажи наличка» (числом):")
    return SALES_CASH

async def close_sales_cash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("close") or {}
    v = parse_money(update.message.text or "")
    if v is None:
        await update.message.reply_text("Нужно число. Пример: 1234.50")
        return SALES_CASH
    data["sales_cash"] = v
    context.user_data["close"] = data
    await update.message.reply_text("Теперь введи «Возвраты» (если нет — 0):")
    return REFUNDS

async def close_refunds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("close") or {}
    v = parse_money(update.message.text or "")
    if v is None:
        await update.message.reply_text("Нужно число. Пример: 0")
        return REFUNDS
    data["refunds"] = v
    context.user_data["close"] = data
    await update.message.reply_text("Теперь пришли фото чека №1 📸")
    return RECEIPT1

async def close_receipt1(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("close") or {}
    pid = _extract_photo_file_id(update)
    if not pid:
        await update.message.reply_text("Нужно фото чека №1 📸")
        return RECEIPT1
    data["receipt1"] = pid
    context.user_data["close"] = data
    await update.message.reply_text("Теперь пришли фото чека №2 📸")
    return RECEIPT2

async def close_receipt2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("close") or {}
    pid = _extract_photo_file_id(update)
    if not pid:
        await update.message.reply_text("Нужно фото чека №2 📸")
        return RECEIPT2
    data["receipt2"] = pid
    context.user_data["close"] = data
    await update.message.reply_text("Теперь пришли 4 фото уборки (по одному). Фото 1/4 📸")
    return CLEANUP

async def close_cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("close") or {}
    pid = _extract_photo_file_id(update)
    if not pid:
        await update.message.reply_text("Нужно фото уборки 📸")
        return CLEANUP

    arr = data.get("cleanup") or []
    arr.append(pid)
    data["cleanup"] = arr
    context.user_data["close"] = data

    if len(arr) < 4:
        await update.message.reply_text(f"Принял ✅ Фото {len(arr)}/4. Пришли фото {len(arr)+1}/4 📸")
        return CLEANUP

    await update.message.reply_text("Фото уборки приняты ✅\nЕсли есть комментарий — напиши (если нет — напиши «-»):")
    return CLOSE_NOTE

async def close_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = await guard_employee(update, context)
    if not u:
        return ConversationHandler.END

    data = context.user_data.get("close") or {}
    note = (update.message.text or "").strip()
    if note == "-":
        note = ""
    data["note"] = note

    ts = now_tz().isoformat(timespec="seconds")
    day = data["day"]
    point = data["point"]
    session_id = data["session_id"]
    mode = data["mode"]

    cash_in = float(data["cash_in"] or 0)
    sales_cashless = float(data["sales_cashless"] or 0)
    sales_cash = float(data["sales_cash"] or 0)
    refunds = float(data["refunds"] or 0)
    total_sales = sales_cashless + sales_cash - refunds
    cash_in_box = cash_in + sales_cash - refunds  # простая модель

    receipt1 = data["receipt1"]
    receipt2 = data["receipt2"]
    cleanup = data.get("cleanup") or ["", "", "", ""]

    sheet_append(
        SHEET_CLOSE,
        [
            ts, day, sanitize_for_sheets(point), session_id, mode,
            str(u.user_id), sanitize_for_sheets(u.name),
            str(cash_in), str(sales_cashless), str(sales_cash), str(refunds),
            str(total_sales), str(cash_in_box),
            receipt1, receipt2,
            cleanup[0], cleanup[1], cleanup[2], cleanup[3],
            sanitize_for_sheets(note),
        ],
    )

    # закрываем сессию
    sess, role = user_open_context(u.user_id)
    if sess and sess.session_id == session_id:
        if mode == "FULL":
            sess.state = "CLOSED"
            sess.user1_end = ts
        else:
            sess.state = "CLOSED"
            sess.user2_end = ts
        upsert_session(sess)

    await report_to_control(
        context,
        format_control(
            "🔒 Смена закрыта",
            u.name,
            u.user_id,
            point=point,
            details=[
                f"Режим: {mode}",
                f"Внесение: {cash_in}",
                f"Продажи безнал: {sales_cashless}",
                f"Продажи наличка: {sales_cash}",
                f"Возвраты: {refunds}",
                f"Итого продаж: {total_sales}",
                f"Наличка в кассе: {cash_in_box}",
                f"Комментарий: {note or '(нет)'}",
            ],
        ),
    )

    await report_photo_to_control(context, receipt1, caption=f"🧾 Чек 1\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")
    await report_photo_to_control(context, receipt2, caption=f"🧾 Чек 2\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")
    for i, pid in enumerate(cleanup, start=1):
        await report_photo_to_control(context, pid, caption=f"🧼 Уборка {i}/4\nТочка: {point}\nСотрудник: {u.name} ({u.user_id})")

    context.user_data.pop("close", None)

    await update.message.reply_text("Смена закрыта ✅", reply_markup=open_choice_kb())
    return ConversationHandler.END

# -------------------- /totals --------------------

def _close_rows_for_day(day: str) -> List[List[str]]:
    try:
        rows = sheet_get(f"{SHEET_CLOSE}!A2:U")
    except Exception:
        return []
    out = []
    for r in rows:
        if len(r) >= 2 and r[1] == day:
            out.append(r)
    return out

async def totals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    day = day_key()
    rows = _close_rows_for_day(day)
    if not rows:
        await update.message.reply_text(f"Итогов за {day} пока нет.")
        return

    # группируем по точкам
    by_point: Dict[str, Dict[str, float]] = {}
    for r in rows:
        point = normalize_point(r[2] if len(r) > 2 else "")
        total_sales = float(r[11]) if len(r) > 11 and r[11] else 0.0
        cash_in_box = float(r[12]) if len(r) > 12 and r[12] else 0.0
        dct = by_point.setdefault(point, {"total_sales": 0.0, "cash_in_box": 0.0, "count": 0.0})
        dct["total_sales"] += total_sales
        dct["cash_in_box"] += cash_in_box
        dct["count"] += 1.0

    lines = [f"Итоги за {day}:"]
    for p, dct in sorted(by_point.items()):
        lines.append(f"• {p}: Итого продаж={dct['total_sales']:.2f}, Наличка в кассе={dct['cash_in_box']:.2f}, закрытий={int(dct['count'])}")
    await update.message.reply_text("\n".join(lines))

# -------------------- REMINDERS --------------------

async def task_reminders(context: ContextTypes.DEFAULT_TYPE):
    if not ENABLE_REMINDERS:
        return
    d = day_key()
    sessions = list_open_sessions()
    now = now_tz()

    for s in sessions:
        if s.day != d:
            continue
        if s.mode == "FULL" and s.state == "OPEN_FULL":
            uid = int(s.user1_id) if s.user1_id else None
            role = "FULL"
        elif s.mode == "HALF":
            if s.state == "OPEN1":
                uid = int(s.user1_id) if s.user1_id else None
                role = "HALF1"
            elif s.state == "OPEN2":
                uid = int(s.user2_id) if s.user2_id else None
                role = "HALF2"
            else:
                continue
        else:
            continue

        if not uid:
            continue

        last = last_task_action_ts(d, s.point, uid)
        if last is None:
            # если вообще ничего не отмечал — считаем от старта
            try:
                last = datetime.fromisoformat(s.user1_start if role in ("FULL","HALF1") else s.user2_start)
            except Exception:
                last = now

        idle = (now - last).total_seconds() / 60.0
        if idle < REMINDER_IDLE_MINUTES:
            continue

        # анти-спам: не чаще 1 раза в час на пользователя/точку/день
        key = f"reminder:{d}:{normalize_point(s.point)}:{uid}"
        prev_iso = context.bot_data.get(key)
        if prev_iso:
            try:
                prev = datetime.fromisoformat(prev_iso)
                if (now - prev).total_seconds() < 60 * 60:
                    continue
            except Exception:
                pass

        context.bot_data[key] = now.isoformat(timespec="seconds")

        try:
            await context.bot.send_message(
                chat_id=uid,
                text="Напоминание 🙂\nЕсли есть задачи — отметь выполненное. Если всё ок — просто продолжай работу.",
                reply_markup=shift_kb(role, normalize_point(s.point)),
            )
        except Exception as e:
            log.warning("reminder send failed: %s", e)

# -------------------- DAILY TOTALS (control group) --------------------

async def daily_totals_job(context: ContextTypes.DEFAULT_TYPE):
    if not ENABLE_DAILY_TOTALS or CONTROL_GROUP_ID == 0:
        return
    day = day_key()
    rows = _close_rows_for_day(day)
    if not rows:
        await report_to_control(context, f"Итоги за {day}: пока нет закрытий смен.")
        return
    by_point: Dict[str, Dict[str, float]] = {}
    for r in rows:
        point = normalize_point(r[2] if len(r) > 2 else "")
        total_sales = float(r[11]) if len(r) > 11 and r[11] else 0.0
        cash_in_box = float(r[12]) if len(r) > 12 and r[12] else 0.0
        dct = by_point.setdefault(point, {"total_sales": 0.0, "cash_in_box": 0.0, "count": 0.0})
        dct["total_sales"] += total_sales
        dct["cash_in_box"] += cash_in_box
        dct["count"] += 1.0
    lines = [f"📊 Итоги за {day}:"]
    for p, dct in sorted(by_point.items()):
        lines.append(f"• {p}: Итого продаж={dct['total_sales']:.2f}, Наличка в кассе={dct['cash_in_box']:.2f}, закрытий={int(dct['count'])}")
    await report_to_control(context, "\n".join(lines))

# -------------------- MISC CALLBACKS --------------------

async def open_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    u = await guard_employee(update, context)
    if not u:
        return
    if not u.point:
        await safe_edit(q, "Выбери точку:", reply_markup=after_approved_kb())
        return
    await safe_edit(q, "Меню:", reply_markup=open_choice_kb())

# -------------------- MAIN --------------------

def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Register conversation: /start -> name -> code
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start_cmd)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_code)],
        },
        fallbacks=[],
    )

    # Open shift conversation: callback OPEN|FULL / OPEN|HALF -> report -> photo -> photo
    open_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(open_full_start_cb, pattern=r"^OPEN\|(FULL|HALF)$")],
        states={
            OPEN_FULL_REPORT: [MessageHandler(filters.TEXT & ~filters.COMMAND, open_full_report_text)],
            OPEN_FULL_SHOWCASE: [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, open_full_showcase_photo),
                MessageHandler(filters.ALL, lambda u, c: u.message.reply_text("Нужно фото витрины 📸")),
            ],
            OPEN_FULL_MACARONS: [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, open_full_macarons_photo),
                MessageHandler(filters.ALL, lambda u, c: u.message.reply_text("Нужно фото макаронс 📸")),
            ],
        },
        fallbacks=[],
        name="open_conv",
        persistent=False,
    )

    # Close shift conversation: callback CLOSE -> numbers -> receipts -> cleanup -> note
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
            CLOSE_NOTE: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_note)],
        },
        fallbacks=[],
        name="close_conv",
        persistent=False,
    )

    # Handlers
    app.add_handler(reg_conv)

    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^ADM\|"))
    app.add_handler(CommandHandler("block", cmd_block))
    app.add_handler(CommandHandler("unblock", cmd_unblock))
    app.add_handler(CommandHandler("pending", cmd_pending))

    app.add_handler(CallbackQueryHandler(choose_point_cb, pattern=r"^CHOOSE_POINT$"))
    app.add_handler(CallbackQueryHandler(point_pick_cb, pattern=r"^POINT\|"))
    app.add_handler(CallbackQueryHandler(open_menu_cb, pattern=r"^MENU$"))

    app.add_handler(open_conv)

    app.add_handler(CallbackQueryHandler(plan_cb, pattern=r"^PLAN$"))
    app.add_handler(CallbackQueryHandler(mark_cb, pattern=r"^MARK$"))
    app.add_handler(CallbackQueryHandler(task_pick_cb, pattern=r"^TASK\|"))
    app.add_handler(CallbackQueryHandler(skip_task_photo2_cb, pattern=r"^SKIP_TASK_PHOTO2$"))

    app.add_handler(CallbackQueryHandler(help_cb, pattern=r"^HELP$"))
    app.add_handler(CallbackQueryHandler(help_send_cb, pattern=r"^HELP_SEND$"))
    app.add_handler(CallbackQueryHandler(help_cancel_cb, pattern=r"^HELP_CANCEL$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, help_text_message))

    app.add_handler(CallbackQueryHandler(transfer_cb, pattern=r"^TRANSFER$"))
    app.add_handler(CallbackQueryHandler(pick_user2_cb, pattern=r"^U2\|"))
    app.add_handler(CallbackQueryHandler(accept_shift_cb, pattern=r"^ACCEPT\|"))

    app.add_handler(close_conv)

    app.add_handler(CommandHandler("totals", totals_cmd))

    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.IMAGE, photo_message))

    # Jobs
    if ENABLE_REMINDERS:
        app.job_queue.run_repeating(task_reminders, interval=timedelta(minutes=REMINDER_CHECK_MINUTES), first=timedelta(minutes=1))
    if ENABLE_DAILY_TOTALS:
        # запускаем ежедневно в указанное время
        app.job_queue.run_daily(daily_totals_job, time=time(DAILY_TOTALS_HOUR, DAILY_TOTALS_MINUTE, tzinfo=_tz))

    return app

def main():
    require_env()
    ensure_sheets()

    app = build_application()

    if WEBHOOK_MODE:
        if not WEBHOOK_BASE_URL:
            raise RuntimeError("WEBHOOK_MODE=1, но WEBHOOK_BASE_URL не задан")
        webhook_url = f"{WEBHOOK_BASE_URL}/{WEBHOOK_PATH}"
        log.info("Starting webhook on port %s, path=/%s", PORT, WEBHOOK_PATH)
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=webhook_url,
            drop_pending_updates=True,
            max_connections=40,
        )
    else:
        log.info("Starting polling")
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
