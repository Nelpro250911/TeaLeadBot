#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tea Lead Finder — демо-скрипт для парсинга OLX по ключам и отправки лидов в Telegram.

⚠️ Важно:
- Соблюдайте условия использования OLX и местные законы.
- Для продакшена добавьте ротацию прокси, капчу-байпас/anti-bot, устойчивые селекторы и логирование.

Функции:
- Парсит OLX по заданным ключам (укр/рус) и фильтрует по городу «Київ/Киев».
- Дедупликация лидов (SQLite) — не присылает одно и то же повторно.
- Telegram-бот на pyTelegramBotAPI: /start, /scan, /status, /help.
- Фоновый мониторинг: периодический скан и автоотправка подписчикам.

Зависимости (requirements.txt):
  pyTelegramBotAPI>=4.28.0
  requests>=2.31.0
  beautifulsoup4>=4.12.0
  lxml>=5.2.1
  fake-useragent>=1.5.1
"""

import os
import re
import time
import sqlite3
import hashlib
import logging
import threading
import argparse
from datetime import datetime
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

try:
    import telebot  # pyTelegramBotAPI
except Exception:
    telebot = None

# -------------------- Конфиг --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "leads.db")
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "360"))  # каждые 6 часов
CITY_NAMES = {"київ", "киев", "kyiv"}

KEYWORDS: List[str] = [
    "куплю чай",
    "куплю чай оптом",
    "травяний чай",
    "чорний чай",
    "ароматизований чай",
    "зелений чай",
    "чай оптом",
    "купить чай оптом",
    "чаї оптом Україна",
    "травяні чаї",
    "чай до кав'ярні",
    "куплю матча чай",
    "чай для подарунка",
    "чайний набір купити",
    "пакетовані чаї оптом",
    "чай оптом Київ",
    "органічний чай купити",
    "чай онлайн",
    "чай доставка Київ",
    "чай гуртом",
    "чай магазин Київ",
    "чай преміум купити",
    "чайний бутик Київ",
]

HEADERS_POOL = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
        "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
    },
]

OLX_SEARCH_URL = "https://www.olx.ua/uk/list/q-{query}/?search%5Border%5D=created_at:desc"
OLX_SEARCH_URL_RU = "https://www.olx.ua/d/list/q-{query}/?search%5Border%5D=created_at:desc"

# -------------------- Логирование --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("tea_lead_finder")

# -------------------- Утилиты --------------------

def hsh(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# -------------------- Хранилище (SQLite) --------------------

def init_db(path: str = DB_PATH):
    # Разрешаем использование соединения в разных потоках
    conn = sqlite3.connect(path, check_same_thread=False)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id TEXT PRIMARY KEY,
            url TEXT,
            title TEXT,
            price TEXT,
            location TEXT,
            published_at TEXT,
            source TEXT,
            keyword TEXT,
            created_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id TEXT PRIMARY KEY,
            created_at TEXT
        )
        """
    )
    # Улучшаем параллельность чтения/записи
    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    return conn


def save_leads(conn, leads: List[Dict]) -> List[Dict]:
    """Сохраняет новые лиды, возвращает только новые."""
    new_items = []
    cur = conn.cursor()
    for lead in leads:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO leads (id, url, title, price, location, published_at, source, keyword, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    lead["id"], lead["url"], lead.get("title"), lead.get("price"), lead.get("location"),
                    lead.get("published_at"), lead.get("source", "olx"), lead.get("keyword", ""), datetime.utcnow().isoformat(),
                ),
            )
            if cur.rowcount == 1:
                new_items.append(lead)
        except Exception as e:
            logger.warning(f"DB insert error for {lead.get('url')}: {e}")
    conn.commit()
    return new_items


def get_subscribers(conn) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM subscribers")
    return [row[0] for row in cur.fetchall()]


def add_subscriber(conn, chat_id: str):
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO subscribers (chat_id, created_at) VALUES (?, ?)", (str(chat_id), datetime.utcnow().isoformat()))
    conn.commit()

# -------------------- Парсер OLX --------------------

def request_with_headers(url: str) -> Optional[requests.Response]:
    for hdr in HEADERS_POOL:
        try:
            r = requests.get(url, headers=hdr, timeout=25)
            if r.status_code == 200:
                return r
            time.sleep(1.0)
        except Exception:
            time.sleep(0.5)
            continue
    return None


def is_kyiv(text: str) -> bool:
    t = (text or "").lower()
    return any(name in t for name in CITY_NAMES)


def parse_olx_list(html: str, keyword: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href:
            continue
        if "/d/" in href and ("/obyavlenie/" in href or "/ogoloshennya/" in href):
            url = href if href.startswith("http") else ("https://www.olx.ua" + href)
            card = a.parent
            title = None
            price = None
            location = None
            published = None
            for _ in range(3):
                if card is None:
                    break
                if not title:
                    h = card.find(["h6", "h5", "h4", "h3"])
                    if h:
                        title = clean_space(h.get_text(" "))
                if not price:
                    price_el = card.find("p")
                    if price_el and any(sym in price_el.get_text() for sym in ["₴", "грн", "$", "€"]):
                        price = clean_space(price_el.get_text(" "))
                if not location or not published:
                    for small in card.find_all(["p", "span"]):
                        txt = clean_space(small.get_text(" "))
                        if not location and txt and ("Київ" in txt or "Киев" in txt or "Kyiv" in txt or "район" in txt):
                            location = txt
                        if not published and any(w in txt.lower() for w in ["сьогодні", "сегодня", "вчора", "вчера", ":", "202", "2024", "2025"]):
                            published = txt
                card = card.parent
            if location and not is_kyiv(location):
                continue
            uid = hsh(url)
            items.append({
                "id": uid,
                "url": url,
                "title": title or "(без назви)",
                "price": price or "—",
                "location": location or "Київ",
                "published_at": published or "",
                "source": "olx",
                "keyword": keyword,
            })
    unique = {}
    for it in items:
        unique.setdefault(it["id"], it)
    return list(unique.values())


def search_olx_by_keyword(keyword: str) -> List[Dict]:
    q = requests.utils.quote(keyword)
    urls = [OLX_SEARCH_URL.format(query=q), OLX_SEARCH_URL_RU.format(query=q)]
    all_items: List[Dict] = []
    for url in urls:
        r = request_with_headers(url)
        if not r:
            logger.warning(f"No response for {url}")
            continue
        parsed = parse_olx_list(r.text, keyword)
        all_items.extend(parsed)
        time.sleep(1.0)
    uniq = {it["id"]: it for it in all_items}
    return list(uniq.values())


def scan_all_keywords() -> List[Dict]:
    all_leads: List[Dict] = []
    for kw in KEYWORDS:
        try:
            items = search_olx_by_keyword(kw)
            all_leads.extend(items)
        except Exception as e:
            logger.warning(f"Scan kw error '{kw}': {e}")
        time.sleep(1.0)
    uniq = {x["id"]: x for x in all_leads}
    return list(uniq.values())

# -------------------- Telegram --------------------

def format_lead(lead: Dict) -> str:
    parts = [
        "📍 Новий потенційний клієнт (OLX)",
        f"🔑 Ключ: {lead.get('keyword', '')}",
        f"🏷️ Назва: {lead.get('title', '—')}",
        f"💵 Ціна: {lead.get('price', '—')}",
        f"📍 Локація: {lead.get('location', '—')}",
        f"🕒 Оновлено: {lead.get('published_at', '')}",
        f"🔗 Посилання: {lead.get('url', '')}",
    ]
    return "\n".join(parts)


def send_to_telegram(bot, chat_id: str, text: str):
    try:
        bot.send_message(chat_id, text, disable_web_page_preview=False)
    except Exception as e:
        logger.error(f"Telegram send error to {chat_id}: {e}")


def run_periodic_scanner(_conn_main_thread):
    if not BOT_TOKEN or telebot is None:
        logger.warning("BOT_TOKEN не задан или pyTelegramBotAPI не установлен — фоновый скан без Telegram.")
    bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None) if BOT_TOKEN and telebot else None

    while True:
        try:
            logger.info("[SCAN] Запуск сканирования по ключам…")
            # локальный конекшен для этого потока
            conn_thread = init_db(DB_PATH)
            leads = scan_all_keywords()
            new_leads = save_leads(conn_thread, leads)
            logger.info(f"[SCAN] Найдено: {len(leads)}; Новых: {len(new_leads)}")
            if bot and new_leads:
                subs = get_subscribers(conn_thread)
                for lead in new_leads:
                    msg = format_lead(lead)
                    for chat_id in subs:
                        send_to_telegram(bot, chat_id, msg)
        except Exception as e:
            logger.error(f"Periodic scan error: {e}")
        finally:
            logger.info(f"Следующий скан через {SCAN_INTERVAL_MIN} мин…")
            time.sleep(SCAN_INTERVAL_MIN * 60)

# -------------------- Бот-команды --------------------

def start_bot(_conn_main_thread):
    if not BOT_TOKEN:
        logger.error("Не задан BOT_TOKEN. Установи переменную окружения и перезапусти.")
        return
    if telebot is None:
        logger.error("pyTelegramBotAPI не установлен. Добавь в requirements.txt и установи.")
        return

    bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

    @bot.message_handler(commands=['start', 'help'])
    def handle_start(m):
        conn_h = init_db(DB_PATH)
        add_subscriber(conn_h, m.chat.id)
        bot.reply_to(m, (
            "Вітаю! Я бот для пошуку лідів по чаю в Києві.\n\n"
            "Команди:\n"
            "/scan — ручний скан зараз\n"
            "/status — статистика в БД\n"
            "/help — довідка\n\n"
            "Я вже додав тебе в підписники. Нові ліди прийдуть автоматично після наступного скану."
        ))

    @bot.message_handler(commands=['scan'])
    def handle_scan(m):
        bot.reply_to(m, "Сканую OLX, зачекай 10–60 сек…")
        try:
            conn_h = init_db(DB_PATH)
            leads = scan_all_keywords()
            new_leads = save_leads(conn_h, leads)
            if not new_leads:
                bot.send_message(m.chat.id, "Нових оголошень не знайдено. Спробую пізніше.")
            else:
                for lead in new_leads[:20]:
                    bot.send_message(m.chat.id, format_lead(lead))
        except Exception as e:
            bot.send_message(m.chat.id, f"Помилка сканування: {e}")

    @bot.message_handler(commands=['status'])
    def handle_status(m):
        conn_h = init_db(DB_PATH)
        cur = conn_h.cursor()
        c = cur.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        subs = len(get_subscribers(conn_h))
        bot.reply_to(m, f"Статистика: {c} лід(ів) у базі. Підписників: {subs}.")

    logger.info("Бот запущен. Ожидаю команды…")
    bot.infinity_polling(skip_pending=True, timeout=30)

# -------------------- Main --------------------

def main():
    parser = argparse.ArgumentParser(description="Tea Lead Finder — OLX demo")
    parser.add_argument("--oneshot", action="store_true", help="Скан и вывод в консоль (без Telegram)")
    args = parser.parse_args()

    # Главный конекшен (не передаем между потоками)
    conn = init_db(DB_PATH)

    if args.oneshot:
        leads = scan_all_keywords()
        new_leads = save_leads(conn, leads)
        print(f"Найдено: {len(leads)}; Новых: {len(new_leads)}")
        for lead in new_leads[:10]:
            print("-" * 60)
            print(format_lead(lead))
        return

    # Фоновый сканер в отдельном потоке
    t = threading.Thread(target=run_periodic_scanner, args=(conn,), daemon=True)
    t.start()

    # Запускаем бота (polling)
    start_bot(conn)


if __name__ == "__main__":
    main()
