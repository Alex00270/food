#!/usr/bin/env python3
import os
import json
import sqlite3
import re
from pathlib import Path

DB_PATH = os.getenv('FOOD_DB_PATH', 'food.db')
PARSED_ROOT = os.getenv('PARSED_ROOT', 'parsed')


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS compare_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reestr_number TEXT,
            report_path TEXT,
            status TEXT,
            created_at TEXT
        )
    ''')
    conn.commit()
    conn.close()


def find_numbers(text):
    if not text:
        return []
    text = text.replace('\xa0', ' ').replace('₽', '').replace('RUB', '')
    text = re.sub(r'\s+', ' ', text)
    nums = re.findall(r"\d+[\d\s]*[\.,]?\d*", text)
    cleaned = []
    for n in nums:
        val = n.replace(' ', '').replace(',', '.')
        try:
            cleaned.append(float(val))
        except Exception:
            continue
    return cleaned


def load_parsed(reestr_number):
    root = Path(PARSED_ROOT) / reestr_number
    if not root.exists():
        return []
    data = []
    for p in root.rglob('*.json'):
        try:
            obj = json.loads(p.read_text(encoding='utf-8'))
            obj['_path'] = str(p)
            data.append(obj)
        except Exception:
            continue
    return data


def summarize(parsed):
    summary = []
    for obj in parsed:
        text = obj.get('text') or ''
        numbers = find_numbers(text)
        summary.append({
            'file': obj.get('file'),
            'type': obj.get('type'),
            'numbers_count': len(numbers),
            'sample_numbers': numbers[:20]
        })
    return summary


def compare(reestr_number):
    parsed = load_parsed(reestr_number)
    summary = summarize(parsed)

    # Very light comparison: pick biggest numbers as possible totals
    totals = []
    for item in summary:
        if item['sample_numbers']:
            totals.append(max(item['sample_numbers']))

    report = {
        'reestr_number': reestr_number,
        'files_checked': len(summary),
        'summary': summary,
        'max_numbers': sorted(totals, reverse=True)[:10]
    }

    report_path = Path(PARSED_ROOT) / reestr_number / 'compare_report.json'
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO compare_reports (reestr_number, report_path, status, created_at) VALUES (?, ?, ?, datetime('now'))",
        (reestr_number, str(report_path), 'created')
    )
    conn.commit()
    conn.close()

    return report_path


def main():
    import sys
    if len(sys.argv) < 2:
        print('Usage: compare_parsed.py <reestr_number>')
        return
    init_db()
    report = compare(sys.argv[1])
    print(f"Report: {report}")


if __name__ == '__main__':
    main()
