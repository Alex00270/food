#!/usr/bin/env python3
import datetime
from bot import get_gc, get_contract_numbers_from_registry, find_existing_contract_sheet_id


def pick_last_data_sheet(sh):
    # Prefer date-named sheets; fallback to last updated order of worksheets list
    sheets = sh.worksheets()
    data_sheets = [ws for ws in sheets if ws.title not in ("Summary", "Registry")]
    if not data_sheets:
        return None

    def parse_date(title):
        try:
            return datetime.date.fromisoformat(title)
        except Exception:
            return None

    dated = [(ws, parse_date(ws.title)) for ws in data_sheets]
    dated_valid = [item for item in dated if item[1] is not None]
    if dated_valid:
        dated_valid.sort(key=lambda x: x[1], reverse=True)
        return dated_valid[0][0]

    # Fallback: last sheet in list
    return data_sheets[-1]


def swap_columns(ws):
    values = ws.get_all_values()
    if not values:
        return False

    # Ensure header
    header = values[0]
    # Set correct header labels
    while len(header) < 4:
        header.append("")
    header[1] = "Цена за единицу"
    header[3] = "Кол-во"
    ws.update("A1:G1", [header[:7]], value_input_option="USER_ENTERED")

    if len(values) < 2:
        return True

    # Swap columns B and D for rows 2..N
    col_b = []
    col_d = []
    for row in values[1:]:
        b = row[1] if len(row) > 1 else ""
        d = row[3] if len(row) > 3 else ""
        col_b.append([d])
        col_d.append([b])

    end_row = len(values)
    ws.update(f"B2:B{end_row}", col_b, value_input_option="USER_ENTERED")
    ws.update(f"D2:D{end_row}", col_d, value_input_option="USER_ENTERED")
    return True


def main():
    gc = get_gc()
    if not gc:
        print("No Google credentials")
        return

    numbers = get_contract_numbers_from_registry()
    if not numbers:
        print("Registry is empty")
        return

    fixed = 0
    for number in numbers:
        sheet_id = find_existing_contract_sheet_id(number)
        if not sheet_id:
            continue
        sh = gc.open_by_key(sheet_id)
        ws = pick_last_data_sheet(sh)
        if not ws:
            continue
        if swap_columns(ws):
            fixed += 1
            print(f"Fixed: {number} -> {ws.title}")

    print(f"Done. Fixed sheets: {fixed}")


if __name__ == '__main__':
    main()
