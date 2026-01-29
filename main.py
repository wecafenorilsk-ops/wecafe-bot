#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
WeCafe Shift & Tasks Bot (Telegram) — версия под сценарий DreamTeam

Основано на текущем работающем боте и его подходе к Google Sheets (users/points/cleaning_schedule + лог-листы).

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


def
