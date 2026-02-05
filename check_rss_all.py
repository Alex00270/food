#!/usr/bin/env python3
import sqlite3
import requests
import xml.etree.ElementTree as ET
import datetime
import os

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
RSS_NOTIFY_CHAT_ID = os.getenv('RSS_NOTIFY_CHAT_ID')

RSS_TRIGGER_PARSE = os.getenv('RSS_TRIGGER_PARSE', 'true').lower() == 'true'

DB_PATH = 'food.db'


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS rss_state (
            reestr_number TEXT PRIMARY KEY,
            feed_url TEXT,
            last_guid TEXT,
            last_pubdate TEXT,
            last_checked TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS rss_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reestr_number TEXT,
            guid TEXT,
            pubdate TEXT,
            title TEXT,
            link TEXT,
            created_at TEXT
        )
    ''')
    conn.commit()
    conn.close()


def get_contracts():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT reestr_number, feed_url, last_guid FROM rss_state")
    rows = cur.fetchall()
    conn.close()
    return rows


def update_state(reestr_number, last_guid, last_pubdate):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE rss_state SET last_guid=?, last_pubdate=?, last_checked=? WHERE reestr_number=?",
        (last_guid, last_pubdate, datetime.datetime.utcnow().isoformat(), reestr_number)
    )
    conn.commit()
    conn.close()


def add_event(reestr_number, guid, pubdate, title, link):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO rss_events (reestr_number, guid, pubdate, title, link, created_at) VALUES (?, ?, ?, ?, ?, datetime('now'))",
        (reestr_number, guid, pubdate, title, link)
    )
    conn.commit()
    conn.close()


def parse_rss(xml_text):
    root = ET.fromstring(xml_text)
    channel = root.find('channel')
    if channel is None:
        return []
    items = []
    for item in channel.findall('item'):
        guid = (item.findtext('guid') or '').strip()
        title = (item.findtext('title') or '').strip()
        link = (item.findtext('link') or '').strip()
        pubdate = (item.findtext('pubDate') or '').strip()
        items.append({'guid': guid, 'title': title, 'link': link, 'pubdate': pubdate})
    return items


def main():
    init_db()
    rows = get_contracts()
    if not rows:
        print('No RSS feeds in db')
        return

    for reestr_number, feed_url, last_guid in rows:
        if not feed_url:
            continue
        try:
            resp = requests.get(feed_url, timeout=30)
            if resp.status_code != 200:
                print(f"{reestr_number}: RSS status {resp.status_code}")
                continue
            items = parse_rss(resp.text)
            if not items:
                continue
            latest = items[0]
            if last_guid and latest['guid'] == last_guid:
                update_state(reestr_number, last_guid, latest['pubdate'])
                continue
            # new event
            add_event(reestr_number, latest['guid'], latest['pubdate'], latest['title'], latest['link'])
            update_state(reestr_number, latest['guid'], latest['pubdate'])
            print(f"{reestr_number}: NEW -> {latest['title']}")

            if RSS_NOTIFY_CHAT_ID and TELEGRAM_TOKEN:
                try:
                    import telebot
                    bot = telebot.TeleBot(TELEGRAM_TOKEN)
                    msg = (
                        f"📰 RSS обновление\\n"
                        f"Контракт: {reestr_number}\\n"
                        f"Дата: {latest['pubdate']}\\n"
                        f"{latest['title']}\\n"
                        f"{latest['link']}"
                    )
                    bot.send_message(int(RSS_NOTIFY_CHAT_ID), msg)
                except Exception as e:
                    print(f"{reestr_number}: notify failed: {e}")

            if RSS_TRIGGER_PARSE:
                try:
                    from bot import check_contract_update
                    check_contract_update(None, reestr_number, silent=True)
                    print(f"{reestr_number}: parse triggered")
                except Exception as e:
                    print(f"{reestr_number}: parse trigger failed: {e}")
        except Exception as e:
            print(f"{reestr_number}: RSS error {e}")


if __name__ == '__main__':
    main()
