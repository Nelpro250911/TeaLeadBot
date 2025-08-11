# Tea Lead Finder (Render deploy)

Швидкий деплой OLX-парсера + Telegram-бота на Render як **Background Worker**.

## Як запустити
1. Створи GitHub репозиторій і завантаж сюди файли.
2. На Render натисни **New → Background Worker** і підключи репозиторій.
3. У Build Command: `pip install -r requirements.txt`
4. У Start Command: `bash start.sh`
5. Додай змінні середовища:
   - `BOT_TOKEN` — токен від @BotFather
   - `SCAN_INTERVAL_MIN` — інтервал скану в хвилинах (за замовчуванням 360)
6. Задеплой і в Telegram натисни **Start** у боті, команда `/scan` — тест.

> Зауваження: безкоштовний план Render може засинати; для 24/7 обери платний план або VPS.
