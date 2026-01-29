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
            await safe_edit(
                q,
                "На этой точке уже открыта полная смена сегодня. Обратись к руководителю.",
                reply_markup=open_choice_kb(),
            )
        else:
            await safe_edit(
                q,
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

    # FIX: не чистим open_shift_mode здесь (он нужен дальше), label берём из mode
    label = "Пол смены" if mode == "HALF" else "Полная смена"
    await safe_edit(
        q,
        f"{label}.\n\n"
        "Перечисли десерты в витрине и сроки их годности:",
    )
    return OPEN_FULL_REPORT


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
    context.user_data.pop("open_shift_mode", None)

    # отчет в контроль: открытие + текст + 2 фото
    details = [f"Время: {ts}"]
    if report_text:
        details.append("Отчет витрины:")
        details.append(report_text[:1500])

    # FIX: используем локальную переменную mode (после pop она бы пропала)
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
    return ConversationHandler.END
