#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tea Lead Finder — OLX parser + Telegram bot.
"""

import os
import re
import time
import sqlite3
import hashlib
import logging
import threading
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup

try:
    import telebot
except Exception:
    telebot = None

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "leads.db")
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "360"))
WALLET_URL = os.getenv("WALLET_URL", "https://your.wallet/link")
CITY_NAMES = {"київ", "киев", "kyiv"}

KEYWORDS: List[str] = [
    "куплю чай", "куплю чай оптом", "травяний чай", "чорний чай", "ароматизований чай",
    "зелений чай", "чай оптом", "купить чай оптом", "чаї оптом Україна", "травяні чаї",
    "чай до кав'ярні", "куплю матча чай", "чай для подарунка", "чайний набір купити",
    "пакетовані чаї оптом", "чай оптом Київ", "органічний чай купити", "чай онлайн",
    "чай доставка Київ", "чай гуртом", "чай магазин Київ", "чай преміум купити",
    "чайний бутик Київ",
]

HEADERS_POOL = [
    {"User-Agent": "Mozilla/5.0", "Accept-Language": "uk-UA,ru;q=0.8,en;q=0.7"},
]

OLX_SEARCH_URL = "https://www.olx.ua/uk/list/q-{query}/?search%5Border%5D=created_at:desc"
OLX_SEARCH_URL_RU = "https://www.olx.ua/d/list/q-{query}/?search%5Border%5D=created_at:desc"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("tea_lead_finder")

def hsh(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

def clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def init_db(path: str = DB_PATH):
    conn = sqlite3.connect(path, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS leads (id TEXT PRIMARY KEY, url TEXT, title TEXT, price TEXT, location TEXT, published_at TEXT, source TEXT, keyword TEXT, created_at TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS subscribers (chat_id TEXT PRIMARY KEY, created_at TEXT)")
    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    return conn

def save_leads(conn, leads: List[Dict]) -> List[Dict]:
    new_items = []
    cur = conn.cursor()
    for lead in leads:
        try:
            cur.execute("INSERT OR IGNORE INTO leads (id, url, title, price, location, published_at, source, keyword, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                lead["id"], lead["url"], lead.get("title"), lead.get("price"), lead.get("location"),
                lead.get("published_at"), lead.get("source", "olx"), lead.get("keyword", ""), datetime.now(timezone.utc).isoformat()
            ))
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
    cur.execute("INSERT OR IGNORE INTO subscribers (chat_id, created_at) VALUES (?, ?)", (str(chat_id), datetime.now(timezone.utc).isoformat()))
    conn.commit()

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
            uid = hsh(url)
            items.append({"id": uid, "url": url, "title": keyword, "price": "—", "location": "Київ", "published_at": "", "source": "olx", "keyword": keyword})
    uniq = {it["id"]: it for it in items}
    return list(uniq.values())

def search_olx_by_keyword(keyword: str) -> List[Dict]:
    q = requests.utils.quote(keyword)
    urls = [OLX_SEARCH_URL.format(query=q), OLX_SEARCH_URL_RU.format(query=q)]
    all_items: List[Dict] = []
    for url in urls:
        r = request_with_headers(url)
        if not r:
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

def format_lead(lead: Dict) -> str:
    parts = [
        "📍 Новий потенційний клієнт (OLX)",
        f"🔑 Ключ: {lead.get('keyword', '')}",
        f"🏷️ Назва: {lead.get('title', '—')}",
        f"💵 Ціна: {lead.get('price', '—')}",
        f"📍 Локація: {lead.get('location', '—')}",
        f"🕒 Оновлено: {lead.get('published_at', '')}",
        f"🔗 Посилання: {lead.get('url', '')}",
        f"💳 Оплата: {WALLET_URL}",
    ]
    return "\n".join(parts)

def _count_day(conn, ymd: str) -> int:
    cur = conn.cursor()
    return cur.execute("SELECT COUNT(*) FROM leads WHERE date(created_at)=?", (ymd,)).fetchone()[0]

def _count_month(conn, ym: str) -> int:
    cur = conn.cursor()
    return cur.execute("SELECT COUNT(*) FROM leads WHERE strftime('%Y-%m', created_at)=?", (ym,)).fetchone()[0]

def run_periodic_scanner(_):
    bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None) if BOT_TOKEN and telebot else None
    while True:
        try:
            conn_thread = init_db(DB_PATH)
            leads = scan_all_keywords()
            new_leads = save_leads(conn_thread, leads)
            if bot and new_leads:
                subs = get_subscribers(conn_thread)
                for lead in new_leads:
                    msg = format_lead(lead)
                    for chat_id in subs:
                        bot.send_message(chat_id, msg)
        except Exception as e:
            logger.error(f"Periodic scan error: {e}")
        finally:
            time.sleep(SCAN_INTERVAL_MIN * 60)

def start_bot(_):
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN missing")
        return
    bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
    try:
        bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    @bot.message_handler(commands=['start', 'help'])
    def handle_start(m):
        conn_h = init_db(DB_PATH)
        add_subscriber(conn_h, m.chat.id)
        bot.reply_to(m, (
            "Вітаю! Я бот для пошуку лідів по чаю.\n"
            "Команди: /scan /status /stats /help\n"
            f"💳 Оплата: {WALLET_URL}"
        ))

    @bot.message_handler(commands=['status'])
    def handle_status(m):
        conn_h = init_db(DB_PATH)
        cur = conn_h.cursor()
        c = cur.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        subs = len(get_subscribers(conn_h))
        bot.reply_to(m, f"В базі {c} лід(ів). Підписників: {subs}. 💳 {WALLET_URL}")

    @bot.message_handler(commands=['scan'])
    def handle_scan(m):
        conn_h = init_db(DB_PATH)
        leads = scan_all_keywords()
        new_leads = save_leads(conn_h, leads)
        if not new_leads:
            bot.send_message(m.chat.id, "Нових оголошень немає.")
        else:
            for lead in new_leads[:20]:
                bot.send_message(m.chat.id, format_lead(lead))

    @bot.message_handler(commands=['stats'])
    def handle_stats(m):
        conn_h = init_db(DB_PATH)
        today = datetime.now(timezone.utc).date()
        ymd_today = today.isoformat()
        ymd_yest = (today.fromordinal(today.toordinal()-1)).isoformat()
        ym_this = today.strftime('%Y-%m')
        prev_month = (today.replace(day=1).fromordinal(today.replace(day=1).toordinal()-1))
        ym_prev = prev_month.strftime('%Y-%m')
        n_today = _count_day(conn_h, ymd_today)
        n_yest = _count_day(conn_h, ymd_yest)
        delta_d = n_today - n_yest
        d_mark = "🟢 +"+str(delta_d) if delta_d>0 else ("🔴 "+str(delta_d) if delta_d<0 else "⚪︎ 0")
        m_this = _count_month(conn_h, ym_this)
        m_prev = _count_month(conn_h, ym_prev)
        delta_m = m_this - m_prev
        m_mark = "🟢 +"+str(delta_m) if delta_m>0 else ("🔴 "+str(delta_m) if delta_m<0 else "⚪︎ 0")
        text = (f"📊 Статистика\nСьогодні: {n_today} {d_mark}\nВчора: {n_yest}\nЦей місяць: {m_this} {m_mark}\nМинул. місяць: {m_prev}\n💳 {WALLET_URL}")
        bot.reply_to(m, text)

    import time as _t
    from telebot.apihelper import ApiTelegramException as _ApiEx
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=30)
        except _ApiEx as e:
            if '409' in str(e):
                logger.error("Conflict 409: another instance running. Sleep 60s...")
                _t.sleep(60)
                continue
            _t.sleep(15)
        except Exception as e:
            logger.error(f"Polling crash: {e}")
            _t.sleep(15)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--oneshot", action="store_true")
    args = parser.parse_args()
    conn = init_db(DB_PATH)
    if args.oneshot:
        leads = scan_all_keywords()
        new_leads = save_leads(conn, leads)
        for lead in new_leads:
            print(format_lead(lead))
        return
    t = threading.Thread(target=run_periodic_scanner, args=(conn,), daemon=True)
    t.start()
    start_bot(conn)

if __name__ == "__main__":
    main()
