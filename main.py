#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WeCafe Cleaning Bot (Telegram) — polling mode (python-telegram-bot 21.x)

Key features implemented for your request:
- Cleaning tasks are for the whole day/point, but RESPONSIBILITY is split into 2 zones (A/B) when two workers cover a day.
- If a worker works the whole shift ("Смена целиком"), they get ALL tasks.
- Reminders every 30 minutes stay, BUT the same "косяк" is sent only ONCE per violation per slot (no repeats every 30 min).
- End-of-day financial report is allowed only after point closing time:
    69 Параллель, Арена: after 22:00
    Кафе Музей: after 19:00
- Includes a tiny HTTP health server for Render Web Service port scan (returns "ok").
- Suppresses httpx/httpcore INFO logs so BOT_TOKEN doesn't appear in logs.
"""

import asyncio
import csv
import json
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, time, timedelta, date
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import StringIO
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Message,
)
from telegram.constants import ParseMode
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

# ----------------- ENV -----------------

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty. Set it in Render -> Environment.")

ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
CONTROL_CHAT_ID = int(os.getenv("CONTROL_CHAT_ID", "0").strip() or "0")

ACCESS_CODE = os.getenv("ACCESS_CODE", "").strip()
if not ACCESS_CODE:
    ACCESS_CODE = "wecafe2026"  # safe default if you forgot

TZ_NAME = os.getenv("TZ", "Asia/Krasnoyarsk").strip()  # Norilsk
TZ = ZoneInfo(TZ_NAME)

SCHEDULE_CSV_URL = os.getenv("SCHEDULE_CSV_URL", "").strip()

END_OF_DAY_TIME = os.getenv("END_OF_DAY_TIME", "22:30").strip()  # daily admin summary time, not closing time gate
REMINDER_INTERVAL_MIN = int(os.getenv("REMINDER_INTERVAL_MIN", "30").strip() or "30")

# Auto-approve window (not the focus right now, but kept for continuity)
AUTO_APPROVE_START = os.getenv("AUTO_APPROVE_START", "09:00").strip()
AUTO_APPROVE_END = os.getenv("AUTO_APPROVE_END", "15:00").strip()

PORT = int(os.getenv("PORT", "10000").strip() or "10000")

POINTS = ["69 Параллель", "Арена", "Кафе Музей"]

POINT_HOURS = {
    "69 Параллель": (time(10, 0), time(22, 0)),
    "Арена": (time(10, 0), time(22, 0)),
    "Кафе Музей": (time(9, 0), time(19, 0)),
}

# ----------------- LOGGING -----------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# --- Security: hide Telegram token from logs (httpx prints request URLs) ---
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

log = logging.getLogger("wecafe-bot")


# ----------------- STORAGE -----------------

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

USERS_PATH = os.path.join(DATA_DIR, "users.json")
SLOTS_PATH = os.path.join(DATA_DIR, "slots.json")
CLEANING_PATH = os.path.join(DATA_DIR, "cleaning.json")
VIOLATIONS_PATH = os.path.join(DATA_DIR, "violations.json")
REPORTS_PATH = os.path.join(DATA_DIR, "reports.json")


def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.error("Failed to load %s: %s", path, e)
        return default


def _save_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_users() -> Dict[str, dict]:
    return _load_json(USERS_PATH, {})


def save_users(users: Dict[str, dict]) -> None:
    _save_json(USERS_PATH, users)


def load_slots() -> Dict[str, dict]:
    return _load_json(SLOTS_PATH, {})


def save_slots(slots: Dict[str, dict]) -> None:
    _save_json(SLOTS_PATH, slots)


def load_cleaning() -> Dict[str, dict]:
    # per day+point state: tasks, split_map, done
    return _load_json(CLEANING_PATH, {})


def save_cleaning(cleaning: Dict[str, dict]) -> None:
    _save_json(CLEANING_PATH, cleaning)


def load_violations() -> Dict[str, dict]:
    # per slot: which violation keys already sent
    return _load_json(VIOLATIONS_PATH, {})


def save_violations(v: Dict[str, dict]) -> None:
    _save_json(VIOLATIONS_PATH, v)


def load_reports() -> Dict[str, dict]:
    # end of day per date+point report
    return _load_json(REPORTS_PATH, {})


def save_reports(r: Dict[str, dict]) -> None:
    _save_json(REPORTS_PATH, r)


# ----------------- HEALTH SERVER (Render) -----------------

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        # silence default http server logs
        return


def start_health_server():
    try:
        server = HTTPServer(("0.0.0.0", PORT), _HealthHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        log.info("Health server listening on port %s", PORT)
    except Exception as e:
        log.error("Health server failed: %s", e)


# ----------------- HELPERS -----------------

def now_tz() -> datetime:
    return datetime.now(TZ)


def today_key() -> str:
    return now_tz().date().isoformat()


def day_point_key(day: str, point: str) -> str:
    return f"{day}::{point}"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def parse_hhmm(s: str) -> time:
    m = re.match(r"^\s*(\d{1,2})\s*:\s*(\d{2})\s*$", s)
    if not m:
        raise ValueError("bad time")
    h = int(m.group(1))
    mi = int(m.group(2))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        raise ValueError("bad time")
    return time(h, mi)


def in_auto_approve_window(dt: datetime) -> bool:
    try:
        start = parse_hhmm(AUTO_APPROVE_START)
        end = parse_hhmm(AUTO_APPROVE_END)
    except Exception:
        return True
    st = dt.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    en = dt.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    return st <= dt <= en


def point_close_time(point: str) -> time:
    return POINT_HOURS.get(point, (time(10, 0), time(22, 0)))[1]


def point_open_time(point: str) -> time:
    return POINT_HOURS.get(point, (time(10, 0), time(22, 0)))[0]


def after_close_now(point: str) -> bool:
    ct = point_close_time(point)
    dt = now_tz()
    close_dt = dt.replace(hour=ct.hour, minute=ct.minute, second=0, microsecond=0)
    return dt >= close_dt


def slot_duration_minutes(start: time, end: time) -> int:
    # assume same day
    dt = now_tz().date()
    s = datetime(dt.year, dt.month, dt.day, start.hour, start.minute, tzinfo=TZ)
    e = datetime(dt.year, dt.month, dt.day, end.hour, end.minute, tzinfo=TZ)
    if e < s:
        e += timedelta(days=1)
    return int((e - s).total_seconds() // 60)


def shift_duration_minutes(point: str) -> int:
    return slot_duration_minutes(point_open_time(point), point_close_time(point))


def safe_md(text: str) -> str:
    # We use HTML parse mode by default to avoid markdown entity issues
    return text


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# ----------------- SCHEDULE LOADING -----------------

def fetch_schedule_csv() -> str:
    if not SCHEDULE_CSV_URL:
        raise RuntimeError("SCHEDULE_CSV_URL is empty")
    resp = requests.get(SCHEDULE_CSV_URL, timeout=25)
    resp.raise_for_status()
    return resp.text


def detect_and_fix_text(s: str) -> str:
    # Sometimes Google CSV is utf-8 but read incorrectly elsewhere.
    # Here we just return as-is; PTB messages are UTF-8.
    return s


def parse_schedule_tasks_for_today(schedule_csv_text: str, point: str, day: date) -> List[str]:
    """
    Expected CSV layout (simple):
    columns: day, point, task
    where day is YYYY-MM-DD or day-of-month number.
    If your Google sheet differs, adapt this function.
    """
    data = schedule_csv_text
    # Try to handle possible encoding artifacts
    if "Ð" in data or "Ñ" in data:
        # likely double-decoded; attempt to fix from latin-1 -> utf-8
        try:
            data = data.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            pass

    f = StringIO(data)
    reader = csv.DictReader(f)
    out = []
    day_iso = day.isoformat()
    day_num = str(day.day)

    for row in reader:
        r_day = (row.get("day") or row.get("date") or row.get("день") or row.get("Дата") or "").strip()
        r_point = (row.get("point") or row.get("точка") or row.get("Точка") or "").strip()
        r_task = (row.get("task") or row.get("задача") or row.get("Задача") or "").strip()

        if not r_task:
            continue

        if r_point and r_point != point:
            continue

        # day match: exact ISO or day-of-month
        if r_day and (r_day != day_iso and r_day != day_num):
            continue

        out.append(detect_and_fix_text(r_task))

    # If the sheet is a matrix by day-of-month on X and tasks on Y, you likely exported a different structure.
    # In that case use the previous version of bot or give a proper CSV export.
    return out


def split_tasks_two_zones(tasks: List[str]) -> Tuple[List[str], List[str]]:
    # alternating split gives more even workload
    a, b = [], []
    for i, t in enumerate(tasks):
        (a if i % 2 == 0 else b).append(t)
    return a, b


# ----------------- CLEANING STATE -----------------

def get_or_init_day_point_cleaning(day: str, point: str) -> dict:
    cleaning = load_cleaning()
    k = day_point_key(day, point)
    if k not in cleaning:
        cleaning[k] = {
            "day": day,
            "point": point,
            "tasks": [],            # full task list for the day+point
            "split": {},            # task_index -> "A"/"B"
            "done": {},             # task_index -> {by, ts, photo_file_id}
        }
        save_cleaning(cleaning)
    return cleaning[k]


def ensure_tasks_loaded(day: str, point: str) -> List[str]:
    cleaning = load_cleaning()
    k = day_point_key(day, point)
    state = cleaning.get(k)
    if not state:
        state = get_or_init_day_point_cleaning(day, point)
        cleaning = load_cleaning()

    if state.get("tasks"):
        return state["tasks"]

    if not SCHEDULE_CSV_URL:
        # fallback demo tasks
        tasks = [
            "Шкафы: прибраться внутри",
            "Кофемашину сверху протереть",
            "Проверка на ротацию продукции",
            "Дверцы шкафчиков и стойка со стороны гостей",
        ]
    else:
        txt = fetch_schedule_csv()
        tasks = parse_schedule_tasks_for_today(txt, point, datetime.fromisoformat(day).date())

    # store tasks + split map
    a, b = split_tasks_two_zones(tasks)
    split = {}
    for idx, _ in enumerate(tasks):
        split[str(idx)] = "A" if idx % 2 == 0 else "B"

    state = {
        "day": day,
        "point": point,
        "tasks": tasks,
        "split": split,
        "done": state.get("done", {}),
    }
    cleaning[k] = state
    save_cleaning(cleaning)
    return tasks


def tasks_for_group(day: str, point: str, group: str) -> List[Tuple[int, str]]:
    tasks = ensure_tasks_loaded(day, point)
    cleaning = load_cleaning()
    state = cleaning[day_point_key(day, point)]
    out = []
    for idx, t in enumerate(tasks):
        g = state["split"].get(str(idx), "A")
        if group == "ALL" or g == group:
            out.append((idx, t))
    return out


def remaining_task_indices(day: str, point: str, group: str) -> List[int]:
    cleaning = load_cleaning()
    state = cleaning.get(day_point_key(day, point))
    if not state or not state.get("tasks"):
        ensure_tasks_loaded(day, point)
        cleaning = load_cleaning()
        state = cleaning[day_point_key(day, point)]

    done = state.get("done", {})
    remaining = []
    for idx, _ in tasks_for_group(day, point, group):
        if str(idx) not in done:
            remaining.append(idx)
    return remaining


def mark_task_done(day: str, point: str, idx: int, by_user_id: int, by_name: str, photo_file_id: str):
    cleaning = load_cleaning()
    k = day_point_key(day, point)
    state = cleaning.get(k)
    if not state:
        ensure_tasks_loaded(day, point)
        cleaning = load_cleaning()
        state = cleaning[k]

    state.setdefault("done", {})
    state["done"][str(idx)] = {
        "by": by_user_id,
        "by_name": by_name,
        "ts": now_tz().isoformat(),
        "photo": photo_file_id,
    }
    cleaning[k] = state
    save_cleaning(cleaning)


# ----------------- SLOT / RESPONSIBILITY -----------------

def new_slot_id() -> str:
    return f"s{int(now_tz().timestamp())}{os.getpid()}"


def get_user(users: Dict[str, dict], user_id: int) -> Optional[dict]:
    return users.get(str(user_id))


def user_name(users: Dict[str, dict], user_id: int) -> str:
    u = get_user(users, user_id)
    return (u or {}).get("name") or f"user{user_id}"


def open_slot_for_user(user_id: int) -> Optional[dict]:
    slots = load_slots()
    for s in slots.values():
        if s.get("user_id") == user_id and s.get("status") == "open":
            return s
    return None


def active_slot_for_point(day: str, point: str) -> List[dict]:
    slots = load_slots()
    out = []
    for s in slots.values():
        if s.get("day") == day and s.get("point") == point:
            out.append(s)
    out.sort(key=lambda x: x.get("opened_at", ""))
    return out


def decide_group_for_slot(day: str, point: str, full_shift: bool) -> str:
    if full_shift:
        return "ALL"
    slots = active_slot_for_point(day, point)
    # Count already created slots today for this point (any status)
    # First slot gets A, second gets B, others default to B.
    count = len(slots)
    if count <= 0:
        return "A"
    if count == 1:
        return "B"
    return "B"


def slot_is_full_shift(point: str, start: time, end: time) -> bool:
    # If duration >= 85% of full shift -> treat as full shift
    dur = slot_duration_minutes(start, end)
    full = shift_duration_minutes(point)
    return dur >= int(full * 0.85)


# ----------------- ACCESS CONTROL -----------------

def is_active(user: dict) -> bool:
    return bool(user and user.get("active", True))


def is_confirmed(user: dict) -> bool:
    return bool(user and user.get("confirmed", False))


def is_limited(user: dict) -> bool:
    # limited users allowed only: choose point, view plan, mark tasks, upload photos
    return bool(user and user.get("limited", False))


def require_user(update: Update) -> dict:
    users = load_users()
    u = users.get(str(update.effective_user.id))
    if not u:
        return {}
    return u


def can_use_feature(user: dict, feature: str) -> bool:
    """
    feature:
      - "choose_point"
      - "plan"
      - "mark"
      - "transfer"
      - "incident"
      - "report"
      - "admin"
    """
    if not user:
        return False
    if is_admin(user.get("id", 0)):
        return True
    if not is_active(user):
        return False

    if is_limited(user):
        return feature in {"choose_point", "plan", "mark"}

    # normal active user
    return True


# ----------------- CONVERSATIONS -----------------

REG_NAME, REG_CODE = range(2)

SLOT_POINT, SLOT_MODE, SLOT_TIME = range(3)

MARK_PICK_TASK, MARK_WAIT_PHOTO = range(2)

TRANSFER_WAIT_COMMENT = 1

REPORT_POINT, REPORT_CASH, REPORT_CASHLESS, REPORT_PHOTO = range(4)


# ----------------- UI -----------------

def main_menu_kb(user: dict) -> InlineKeyboardMarkup:
    btns = []
    btns.append([InlineKeyboardButton("🏷 Выбрать точку", callback_data="choose_point")])
    btns.append([InlineKeyboardButton("🧹 План уборки сегодня", callback_data="plan_today")])
    btns.append([InlineKeyboardButton("✅ Отметить выполненное (с фото)", callback_data="mark_tasks")])

    # Limited users stop here
    if not is_limited(user):
        btns.append([InlineKeyboardButton("🔁 Передать точку (закрыть слот)", callback_data="transfer_point")])
        btns.append([InlineKeyboardButton("🧾 Фин. отчёт (закрытие дня)", callback_data="fin_report")])
        btns.append([InlineKeyboardButton("💬 Инцидент / Комментарий", callback_data="incident")])

    if is_admin(user.get("id", 0)):
        btns.append([InlineKeyboardButton("🛠 Админка", callback_data="admin")])

    return InlineKeyboardMarkup(btns)


def points_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(p, callback_data=f"{prefix}:{p}")] for p in POINTS])


# ----------------- START / REGISTER -----------------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = load_users()
    uid = update.effective_user.id
    u = users.get(str(uid))

    if u and is_active(u):
        await update.message.reply_text(
            "Меню:",
            reply_markup=main_menu_kb(u)
        )
        return ConversationHandler.END

    await update.message.reply_text("Привет! Напиши своё имя (как тебя записать).", reply_markup=ReplyKeyboardRemove())
    return REG_NAME


async def reg_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = (update.message.text or "").strip()
    if len(name) < 2:
        await update.message.reply_text("Напиши имя нормально (минимум 2 буквы).")
        return REG_NAME

    context.user_data["reg_name"] = name
    await update.message.reply_text("Теперь введи код доступа.")
    return REG_CODE


async def reg_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = (update.message.text or "").strip()
    uid = update.effective_user.id
    name = context.user_data.get("reg_name", update.effective_user.first_name)

    if code != ACCESS_CODE:
        await update.message.reply_text("Код неверный. Попробуй ещё раз.")
        return REG_CODE

    users = load_users()
    dt = now_tz()

    # Auto-approve limited users in window
    auto = in_auto_approve_window(dt)
    users[str(uid)] = {
        "id": uid,
        "name": name,
        "active": True,
        "confirmed": True,    # you asked auto-activation with locks; confirmed=True but limited=True still
        "limited": True,      # locks: only choose/plan/mark
        "registered_at": dt.isoformat(),
        "auto_approved": auto,
    }
    save_users(users)

    # notify control group
    if CONTROL_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=f"✅ Новый сотрудник активирован: {name} ({uid}) [ограниченный доступ]"
            )
        except Exception:
            pass

    await update.message.reply_text(
        "✅ Ок! Ты добавлен.\n"
        "Сейчас доступно: выбрать точку, смотреть план уборки и отмечать задачи с фото.\n\n"
        "Открой меню:",
        reply_markup=main_menu_kb(users[str(uid)])
    )
    return ConversationHandler.END


# ----------------- POINT SELECTION -----------------

async def choose_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = require_user(update)
    if not can_use_feature(user, "choose_point"):
        await q.edit_message_text("Нет доступа.")
        return

    await q.edit_message_text("Выбери точку:", reply_markup=points_kb("set_point"))


async def set_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    point = q.data.split(":", 1)[1]
    users = load_users()
    uid = update.effective_user.id
    u = users.get(str(uid))
    if not u:
        await q.edit_message_text("Нужно /start и регистрация.")
        return

    u["point"] = point
    users[str(uid)] = u
    save_users(users)

    await q.edit_message_text(f"✅ Точка установлена: <b>{html_escape(point)}</b>\n\nМеню:", parse_mode=ParseMode.HTML)
    await q.message.reply_text("Меню:", reply_markup=main_menu_kb(u))


# ----------------- PLAN TODAY -----------------

def get_user_group_for_today(uid: int, day: str, point: str) -> str:
    # If user has an open slot today on that point, use its group. Otherwise default:
    slots = load_slots()
    for s in slots.values():
        if s.get("day") == day and s.get("point") == point and s.get("user_id") == uid:
            if s.get("status") == "open":
                return s.get("group", "A")
    # If no slot, we still show something: default A
    return "A"


async def plan_today_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = require_user(update)
    if not can_use_feature(user, "plan"):
        await q.edit_message_text("Нет доступа.")
        return

    point = user.get("point")
    if not point:
        await q.edit_message_text("Сначала выбери точку.", reply_markup=points_kb("set_point"))
        return

    day = today_key()
    group = get_user_group_for_today(update.effective_user.id, day, point)
    tasks = tasks_for_group(day, point, group)
    cleaning = load_cleaning()[day_point_key(day, point)]
    done = cleaning.get("done", {})

    lines = [f"<b>План на сегодня ({html_escape(point)})</b>", f"Зона ответственности: <b>{group}</b>"]
    for idx, t in tasks:
        mark = "✅" if str(idx) in done else "⬜️"
        lines.append(f"{mark} {html_escape(t)}")

    rem = remaining_task_indices(day, point, group)
    lines.append("")
    lines.append(f"Осталось: <b>{len(rem)}</b>")

    await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user))


# ----------------- MARK TASKS -----------------

async def mark_tasks_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = require_user(update)
    if not can_use_feature(user, "mark"):
        await q.edit_message_text("Нет доступа.")
        return

    point = user.get("point")
    if not point:
        await q.edit_message_text("Сначала выбери точку.", reply_markup=points_kb("set_point"))
        return

    day = today_key()
    group = get_user_group_for_today(update.effective_user.id, day, point)

    rem = remaining_task_indices(day, point, group)
    if not rem:
        await q.edit_message_text("✅ Всё по твоей зоне закрыто. Отлично.", reply_markup=main_menu_kb(user))
        # clear violation once resolved
        clear_violation_for_user_slot(update.effective_user.id, "cleaning_pending")
        return ConversationHandler.END

    # build buttons for remaining tasks
    cleaning = load_cleaning()[day_point_key(day, point)]
    tasks = cleaning["tasks"]
    buttons = []
    for idx in rem[:30]:
        title = tasks[idx]
        buttons.append([InlineKeyboardButton(title[:50], callback_data=f"do_task:{idx}")])

    await q.edit_message_text(
        f"Выбери задачу, которую сделал (потом попросит фото). Зона: {group}",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    context.user_data["mark_point"] = point
    context.user_data["mark_day"] = day
    context.user_data["mark_group"] = group
    return MARK_PICK_TASK


async def pick_task_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    idx = int(data.split(":", 1)[1])
    context.user_data["mark_idx"] = idx
    await q.edit_message_text("Теперь пришли 1 фото (после выполнения задачи).")
    return MARK_WAIT_PHOTO


async def mark_wait_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.message
    if not msg.photo:
        await msg.reply_text("Нужно именно фото. Пришли фото ещё раз.")
        return MARK_WAIT_PHOTO

    file_id = msg.photo[-1].file_id  # best quality
    uid = update.effective_user.id
    users = load_users()
    name = user_name(users, uid)

    day = context.user_data.get("mark_day", today_key())
    point = context.user_data.get("mark_point")
    group = context.user_data.get("mark_group", "A")
    idx = int(context.user_data.get("mark_idx"))

    if not point:
        await msg.reply_text("Сначала выбери точку.")
        return ConversationHandler.END

    # Enforce responsibility: user can only close tasks from their group (unless admin)
    cleaning = load_cleaning()[day_point_key(day, point)]
    task_group = cleaning["split"].get(str(idx), "A")
    if group != "ALL" and task_group != group and not is_admin(uid):
        await msg.reply_text("⚠️ Это задача другой зоны. Закрывай только свои задачи.")
        return ConversationHandler.END

    mark_task_done(day, point, idx, uid, name, file_id)

    # resolve violation flag for this slot if no remaining
    if not remaining_task_indices(day, point, group):
        clear_violation_for_user_slot(uid, "cleaning_pending")

    await msg.reply_text("✅ Принято. Задача закрыта с фото.")
    # show menu
    u = users.get(str(uid), {})
    await msg.reply_text("Меню:", reply_markup=main_menu_kb(u))
    return ConversationHandler.END


# ----------------- VIOLATION (NO SPAM) -----------------

def violation_key(slot_id: str, vtype: str) -> str:
    return f"{slot_id}::{vtype}"


def already_sent(slot_id: str, vtype: str) -> bool:
    v = load_violations()
    sent = v.get("sent", {})
    return bool(sent.get(violation_key(slot_id, vtype)))


def set_sent(slot_id: str, vtype: str, value: bool = True):
    v = load_violations()
    sent = v.setdefault("sent", {})
    key = violation_key(slot_id, vtype)
    if value:
        sent[key] = now_tz().isoformat()
    else:
        sent.pop(key, None)
    v["sent"] = sent
    save_violations(v)


def clear_violation_for_user_slot(uid: int, vtype: str):
    slot = open_slot_for_user(uid)
    if not slot:
        return
    set_sent(slot["id"], vtype, value=False)


# ----------------- SLOT / TRANSFER / REPORT -----------------

async def transfer_point_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = require_user(update)
    if not can_use_feature(user, "transfer"):
        await q.edit_message_text("Нет доступа.")
        return ConversationHandler.END

    slot = open_slot_for_user(update.effective_user.id)
    if not slot:
        await q.edit_message_text("У тебя нет открытого слота.")
        return ConversationHandler.END

    await q.edit_message_text(
        "Напиши коротко: кому передал точку / комментарий по смене.",
        reply_markup=None,
    )
    return TRANSFER_WAIT_COMMENT


async def transfer_wait_comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id
    users = load_users()
    name = user_name(users, uid)

    slot = open_slot_for_user(uid)
    if not slot:
        await update.message.reply_text("Слот уже закрыт.")
        return ConversationHandler.END

    slots = load_slots()
    slot["status"] = "closed"
    slot["ended_at"] = now_tz().isoformat()
    slot["transfer_comment"] = text
    slots[slot["id"]] = slot
    save_slots(slots)

    # notify control
    if CONTROL_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=f"🔁 Передача точки: {slot['point']} | {name} ({uid})\nКомментарий: {text}",
            )
        except Exception:
            pass

    await update.message.reply_text("✅ Ок, слот закрыт. Спасибо.")
    await update.message.reply_text("Меню:", reply_markup=main_menu_kb(users.get(str(uid), {})))
    return ConversationHandler.END


async def fin_report_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = require_user(update)
    if not can_use_feature(user, "report"):
        await q.edit_message_text("Нет доступа.")
        return ConversationHandler.END

    point = user.get("point")
    if not point:
        await q.edit_message_text("Сначала выбери точку.", reply_markup=points_kb("set_point"))
        return ConversationHandler.END

    # Gate: only after close time
    if not after_close_now(point):
        close_t = point_close_time(point).strftime("%H:%M")
        await q.edit_message_text(
            f"⛔ Фин. отчёт можно сдавать только после закрытия точки.\n"
            f"Для <b>{html_escape(point)}</b> это после <b>{close_t}</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(user),
        )
        return ConversationHandler.END

    day = today_key()
    # start report flow
    context.user_data["r_point"] = point
    context.user_data["r_day"] = day

    await q.edit_message_text(f"Фин. отчёт ({html_escape(point)}). Введи сумму НАЛИЧКА (число).", parse_mode=ParseMode.HTML)
    return REPORT_CASH


async def report_cash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip().replace(" ", "").replace(",", ".")
    if not re.match(r"^\d+(\.\d+)?$", t):
        await update.message.reply_text("Нужно число. Например 12345")
        return REPORT_CASH
    context.user_data["r_cash"] = float(t)
    await update.message.reply_text("Введи сумму БЕЗНАЛ (число).")
    return REPORT_CASHLESS


async def report_cashless(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip().replace(" ", "").replace(",", ".")
    if not re.match(r"^\d+(\.\d+)?$", t):
        await update.message.reply_text("Нужно число. Например 12345")
        return REPORT_CASHLESS
    context.user_data["r_cashless"] = float(t)
    await update.message.reply_text("Пришли 1-2 фото чеков (можно одним сообщением). Начни с первого фото.")
    context.user_data["r_photos"] = []
    return REPORT_PHOTO


async def report_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg.photo:
        await msg.reply_text("Нужно фото. Пришли фото чека.")
        return REPORT_PHOTO

    file_id = msg.photo[-1].file_id
    photos = context.user_data.get("r_photos", [])
    photos.append(file_id)
    context.user_data["r_photos"] = photos

    if len(photos) < 2:
        await msg.reply_text("✅ Принял. Если есть ещё фото чека — пришли второе. Если нет — напиши: ГОТОВО")
        return REPORT_PHOTO

    await msg.reply_text("✅ Принял 2 фото. Напиши: ГОТОВО")
    return REPORT_PHOTO


async def report_photo_text_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if (update.message.text or "").strip().lower() not in {"готово", "done", "ok"}:
        await update.message.reply_text("Если фото больше нет — напиши: ГОТОВО")
        return REPORT_PHOTO

    uid = update.effective_user.id
    users = load_users()
    name = user_name(users, uid)

    point = context.user_data.get("r_point")
    day = context.user_data.get("r_day", today_key())
    cash = context.user_data.get("r_cash", 0.0)
    cashless = context.user_data.get("r_cashless", 0.0)
    photos = context.user_data.get("r_photos", [])

    total = cash + cashless

    reports = load_reports()
    k = day_point_key(day, point)
    reports[k] = {
        "day": day,
        "point": point,
        "cash": cash,
        "cashless": cashless,
        "total": total,
        "photos": photos,
        "by": uid,
        "by_name": name,
        "ts": now_tz().isoformat(),
    }
    save_reports(reports)

    # close user's open slot if exists
    slot = open_slot_for_user(uid)
    if slot:
        slots = load_slots()
        slot["status"] = "closed"
        slot["ended_at"] = now_tz().isoformat()
        slot["closed_by_report"] = True
        slots[slot["id"]] = slot
        save_slots(slots)

    # notify control
    if CONTROL_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=CONTROL_CHAT_ID,
                text=(
                    f"🧾 Фин. отчёт: {point}\n"
                    f"Наличка: {cash:.2f}\n"
                    f"Безнал: {cashless:.2f}\n"
                    f"Итого: {total:.2f}\n"
                    f"Сдал: {name} ({uid})"
                ),
            )
        except Exception:
            pass

        # forward photo ids are not accessible here; admin can open in Telegram if needed.

    await update.message.reply_text("✅ Фин. отчёт принят. Спасибо.")
    await update.message.reply_text("Меню:", reply_markup=main_menu_kb(users.get(str(uid), {})))
    return ConversationHandler.END


# ----------------- REMINDER JOB (no spam) -----------------

async def reminder_job(context: ContextTypes.DEFAULT_TYPE):
    slots = load_slots()
    if not slots:
        return

    users = load_users()
    day = today_key()

    for slot_id, slot in list(slots.items()):
        if slot.get("status") != "open":
            continue

        uid = slot.get("user_id")
        point = slot.get("point")
        group = slot.get("group", "A")

        u = users.get(str(uid))
        if not u or not is_active(u):
            continue

        # Only remind about cleaning tasks (and only once per violation)
        rem = remaining_task_indices(day, point, group)
        if rem:
            if not already_sent(slot_id, "cleaning_pending"):
                # message once
                cleaning = load_cleaning()[day_point_key(day, point)]
                tasks = cleaning["tasks"]
                # show a short list of remaining tasks (max 5)
                sample = [tasks[i] for i in rem[:5]]
                msg = (
                    "⚠️ Косяк я снял (не закрыта уборка по твоей зоне) и доложу руководителю.\n"
                    f"Точка: {point}\n"
                    f"Зона: {group}\n"
                    "Осталось:\n• " + "\n• ".join(sample[:5])
                )
                try:
                    await context.bot.send_message(chat_id=uid, text=msg)
                except Exception:
                    pass
                set_sent(slot_id, "cleaning_pending", True)
        else:
            # resolved -> clear, so if it appears again later (rare), it can notify again
            set_sent(slot_id, "cleaning_pending", False)


# ----------------- ADMIN (simple) -----------------

async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = update.effective_user.id
    if not is_admin(uid):
        await q.edit_message_text("Нет доступа.")
        return

    await q.edit_message_text(
        "Админка:\n"
        "/block <user_id> — заблокировать\n"
        "/unblock <user_id> — разблокировать\n"
        "/promote <user_id> — снять limited (полный доступ)\n"
        "/chatid — узнать chat_id группы\n",
    )


async def chatid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(f"chat_id: {chat.id}")


async def block_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    if not context.args:
        await update.message.reply_text("Используй: /block 123456")
        return
    tid = context.args[0]
    users = load_users()
    u = users.get(str(tid))
    if not u:
        await update.message.reply_text("Пользователь не найден.")
        return
    u["active"] = False
    users[str(tid)] = u
    save_users(users)
    await update.message.reply_text(f"✅ Заблокирован {tid}")


async def unblock_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    if not context.args:
        await update.message.reply_text("Используй: /unblock 123456")
        return
    tid = context.args[0]
    users = load_users()
    u = users.get(str(tid))
    if not u:
        await update.message.reply_text("Пользователь не найден.")
        return
    u["active"] = True
    users[str(tid)] = u
    save_users(users)
    await update.message.reply_text(f"✅ Разблокирован {tid}")


async def promote_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    if not context.args:
        await update.message.reply_text("Используй: /promote 123456")
        return
    tid = context.args[0]
    users = load_users()
    u = users.get(str(tid))
    if not u:
        await update.message.reply_text("Пользователь не найден.")
        return
    u["limited"] = False
    u["confirmed"] = True
    users[str(tid)] = u
    save_users(users)
    await update.message.reply_text(f"✅ Полный доступ выдан {tid}")


# ----------------- ROUTING -----------------

async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # if user types random text, just show menu (non-intrusive)
    user = require_user(update)
    if user and is_active(user):
        await update.message.reply_text("Меню:", reply_markup=main_menu_kb(user))
    else:
        await update.message.reply_text("Нужно /start")


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # conversations
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("start", start_cmd)],
        states={
            REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            REG_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_code)],
        },
        fallbacks=[],
    )

    mark_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(mark_tasks_cb, pattern=r"^mark_tasks$")],
        states={
            MARK_PICK_TASK: [CallbackQueryHandler(pick_task_cb, pattern=r"^do_task:\d+$")],
            MARK_WAIT_PHOTO: [MessageHandler(filters.PHOTO, mark_wait_photo)],
        },
        fallbacks=[],
        per_message=False,
    )

    transfer_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(transfer_point_cb, pattern=r"^transfer_point$")],
        states={
            TRANSFER_WAIT_COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, transfer_wait_comment)]
        },
        fallbacks=[],
        per_message=False,
    )

    report_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(fin_report_cb, pattern=r"^fin_report$")],
        states={
            REPORT_CASH: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_cash)],
            REPORT_CASHLESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_cashless)],
            REPORT_PHOTO: [
                MessageHandler(filters.PHOTO, report_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_photo_text_done),
            ],
        },
        fallbacks=[],
        per_message=False,
    )

    # callbacks
    app.add_handler(reg_conv)
    app.add_handler(CallbackQueryHandler(choose_point_cb, pattern=r"^choose_point$"))
    app.add_handler(CallbackQueryHandler(set_point_cb, pattern=r"^set_point:"))
    app.add_handler(CallbackQueryHandler(plan_today_cb, pattern=r"^plan_today$"))
    app.add_handler(mark_conv)
    app.add_handler(transfer_conv)
    app.add_handler(report_conv)
    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^admin$"))

    # admin commands
    app.add_handler(CommandHandler("chatid", chatid_cmd))
    app.add_handler(CommandHandler("block", block_cmd))
    app.add_handler(CommandHandler("unblock", unblock_cmd))
    app.add_handler(CommandHandler("promote", promote_cmd))

    # fallback
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))

    # job queue reminders
    if app.job_queue:
        app.job_queue.run_repeating(reminder_job, interval=REMINDER_INTERVAL_MIN * 60, first=30)
    else:
        log.warning("JobQueue not available. Ensure requirements include python-telegram-bot[job-queue].")

    return app


def main():
    start_health_server()
    app = build_app()
    log.info("Starting bot polling...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
