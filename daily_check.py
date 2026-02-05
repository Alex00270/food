#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from bot import init_db, get_contract_numbers_from_db, check_contract_update

load_dotenv()

def main():
    init_db()
    numbers = get_contract_numbers_from_db()
    if not numbers:
        print("Registry is empty. Nothing to check.")
        return

    processed = 0
    changed = 0
    for number in numbers:
        ok, did_change = check_contract_update(None, number, silent=True)
        if ok:
            processed += 1
            if did_change:
                changed += 1
    print(f"Done. Total: {processed}, changed: {changed}")

if __name__ == '__main__':
    main()
