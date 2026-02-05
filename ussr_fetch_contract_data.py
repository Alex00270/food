import sys
import logging
import asyncio
import json
import re
import hashlib
from playwright.async_api import async_playwright

REQUIISITE_KEYWORDS = [
    "лицевой счет",
    "лицевой счёт",
    "расчетный счет",
    "расчётный счет",
    "бик",
    "к/с",
    "корр",
    "уфк",
    "банк россии",
]

PRICE_SELECTORS = [
    ".price-block__price",
    ".cardMainInfo__contentCost",
    "[data-bind=\"text: price\"]",
    ".contract-price",
    ".contract-card__price",
]


def detect_category(name):
    """
    Определяет категорию питания по названию услуги.
    """
    name_lower = name.lower()

    if re.search(r"1[- ]4|начальн", name_lower):
        return "1-4 классы"
    if re.search(r"овз|ограничен", name_lower):
        # ОВЗ приоритетнее, так как бывает "ОВЗ 1-4"
        return "ОВЗ"
    if re.search(r"5[- ]9|5[- ]11|старш", name_lower):
        return "5-11 классы"
    if re.search(r"гпд|продлен", name_lower):
        return "ГПД"
    if "завтрак" in name_lower:
        return "Завтрак"
    if "обед" in name_lower:
        return "Обед"

    return "Прочее"


def clean_number(value_str):
    """
    Нормализация числовых значений из строк.
    Возвращает float или 0.0, если число не найдено.
    """
    if not value_str:
        return 0.0

    # Берем первую строку, где чаще всего находится значение
    line = value_str.split("\n")[0]
    line = line.replace("\xa0", " ")
    line = line.replace("₽", "").replace("RUB", "")
    line = line.replace("Ставка НДС: 20%", "").replace("Ставка НДС: Без НДС", "")
    line = line.replace(" ", "").replace("\t", "")
    line = line.replace(",", ".")

    match = re.search(r"(\d+\.?\d*)", line)
    if not match:
        return 0.0

    try:
        return float(match.group(1))
    except ValueError:
        return 0.0


def is_requisite_row(name, price_text, total_text):
    text = " ".join([
        name or "",
        price_text or "",
        total_text or "",
    ]).lower()

    if any(keyword in text for keyword in REQUIISITE_KEYWORDS):
        return True

    # Частый случай: реквизиты в строках с нулевой суммой
    if clean_number(total_text) == 0 and re.search(r"\b\d{9}\b|\b\d{20}\b", text):
        return True

    return False


def extract_requisites(raw_blocks):
    """
    Извлекает реквизиты из сырых строк/блоков.
    """
    raw_text = "\n".join([block for block in raw_blocks if block])
    raw_text = raw_text.replace("\xa0", " ")

    requisites = {
        "bank_name": "",
        "bik": "",
        "account": "",
        "corr_account": "",
        "treasury_account": "",
        "inn": "",
        "kpp": "",
        "raw_text": raw_text.strip(),
    }

    if not raw_text:
        return requisites

    bik_match = re.search(r"БИК\s*[:\s]*([0-9]{9})", raw_text, re.IGNORECASE)
    if bik_match:
        requisites["bik"] = bik_match.group(1)

    corr_match = re.search(r"(К/С|Корр\.\s*счет|Корр\.\s*сч[её]т)\s*[:\s]*([0-9]{20})", raw_text, re.IGNORECASE)
    if corr_match:
        requisites["corr_account"] = corr_match.group(2)

    acc_match = re.search(r"(Р/С|Расчетный\s*счет|Расч[её]тный\s*счет)\s*[:\s]*([0-9]{20})", raw_text, re.IGNORECASE)
    if acc_match:
        requisites["account"] = acc_match.group(2)

    treasury_match = re.search(r"Лицев[оы]й\s*сч[её]т[^0-9]*([0-9]{20})", raw_text, re.IGNORECASE)
    if treasury_match:
        requisites["treasury_account"] = treasury_match.group(1)

    inn_match = re.search(r"\bИНН\b\s*[:\s]*([0-9]{10,12})", raw_text, re.IGNORECASE)
    if inn_match:
        requisites["inn"] = inn_match.group(1)

    kpp_match = re.search(r"\bКПП\b\s*[:\s]*([0-9]{9})", raw_text, re.IGNORECASE)
    if kpp_match:
        requisites["kpp"] = kpp_match.group(1)

    # Банк (если удалось вытащить строкой)
    bank_match = re.search(r"(ПАО|АО|ООО|ФК|УФК)[^\n]{3,120}", raw_text)
    if bank_match:
        requisites["bank_name"] = bank_match.group(0).strip()

    return requisites


def stable_hash(value):
    dump = json.dumps(value, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(dump.encode("utf-8")).hexdigest()


async def extract_price_from_page(page):
    for selector in PRICE_SELECTORS:
        try:
            price_el = page.locator(selector).first
            if await price_el.count() > 0:
                text = await price_el.inner_text()
                if clean_number(text) > 0:
                    return text
        except Exception:
            continue
    return ""


async def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No URL provided"}))
        sys.exit(1)

    url = sys.argv[1]
    # Normalize URL
    if 'reestrNumber=' in url and 'contractCard' in url and 'common-info' not in url:
        reestr_match = re.search(r'reestrNumber=(\d+)', url)
        if reestr_match:
            url = f"https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber={reestr_match.group(1)}"

    results = {
        "reestr_number": "",
        "customer": "",
        "price": "",
        "price_clean": 0.0,
        "price_source": "",
        "objects_total_clean": 0.0,
        "date_start": "",
        "date_end": "",
        "objects": [],
        "requisites": {},
        "objects_hash": "",
        "requisites_hash": "",
        "execution": {
            "paid": "0",
            "accepted": "0"
        },
        "url": url
    }

    requisites_raw_blocks = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # 1. Common Info
            await page.goto(url, timeout=60000, wait_until="domcontentloaded")

            try:
                match = re.search(r'reestrNumber=(\d+)', page.url)
                if match:
                    results['reestr_number'] = match.group(1)
            except:
                pass

            try:
                customer_el = page.locator("a[href*='organization']").first
                if await customer_el.count() > 0:
                    results['customer'] = await customer_el.inner_text()
                else:
                    lbl = page.get_by_text("Заказчик", exact=False).first
                    if await lbl.count() > 0:
                        results['customer'] = await lbl.locator("..").inner_text()
            except:
                pass

            try:
                price_text = await extract_price_from_page(page)
                if price_text:
                    results['price'] = price_text
                    results['price_clean'] = clean_number(price_text)
                    results['price_source'] = "page"
            except:
                pass

            try:
                start_lbl = page.get_by_text("Дата начала исполнения контракта").first
                if await start_lbl.count() > 0:
                    results['date_start'] = await start_lbl.locator("xpath=following-sibling::span|following-sibling::div").first.inner_text()

                end_lbl = page.get_by_text("Дата окончания исполнения контракта").first
                if await end_lbl.count() > 0:
                    results['date_end'] = await end_lbl.locator("xpath=following-sibling::span|following-sibling::div").first.inner_text()
            except:
                pass

            # 2. Objects
            try:
                obj_tab = page.locator("a[href*='payment-info-and-target-of-order']").first
                if await obj_tab.count() == 0:
                    obj_tab = page.get_by_text("Информация о контракте").first

                if await obj_tab.count() > 0:
                    href = await obj_tab.get_attribute("href")
                    if href:
                        if not href.startswith("http"):
                            href = "https://zakupki.gov.ru" + href
                        await page.goto(href, wait_until="domcontentloaded")
                    else:
                        await obj_tab.click()
                        await page.wait_for_load_state("domcontentloaded")

                    await page.wait_for_timeout(2000)

                    rows = page.locator(".tableBlock tbody tr")
                    count = await rows.count()

                    for i in range(count):
                        row = rows.nth(i)
                        cells = row.locator("td")
                        if await cells.count() > 3:
                            row_texts = await cells.all_inner_texts()
                            if len(row_texts) >= 5:
                                name = row_texts[1].replace('\n', ' ').strip()
                                price_val = row_texts[4].strip() if len(row_texts) > 4 else "0"
                                total_val = row_texts[6].strip() if len(row_texts) > 6 else "0"

                                if is_requisite_row(name, price_val, total_val):
                                    requisites_raw_blocks.append(" ".join([name, price_val, total_val]))
                                    continue

                                category = detect_category(name)
                                results['objects'].append({
                                    "name": name,
                                    "category": category,
                                    "price": price_val,
                                    "total": total_val
                                })
            except:
                pass

            # 3. Execution
            try:
                exec_tab = page.locator("a[href*='process-info']").first
                if await exec_tab.count() == 0:
                    exec_tab = page.get_by_text("Исполнение").first

                if await exec_tab.count() > 0:
                    href = await exec_tab.get_attribute("href")
                    if href:
                        if not href.startswith("http"):
                            href = "https://zakupki.gov.ru" + href
                        await page.goto(href, wait_until="domcontentloaded")
                    else:
                        await exec_tab.click()
                        await page.wait_for_load_state("domcontentloaded")

                    await page.wait_for_timeout(2000)

                    paid_lbl = page.get_by_text("Фактически оплачено").first
                    if await paid_lbl.count() > 0:
                        val = await paid_lbl.locator("xpath=..").inner_text()
                        val = val.replace("Фактически оплачено", "").replace("₽", "").strip()
                        results['execution']['paid'] = val

                    done_lbl = page.get_by_text("Стоимость исполненных обязательств").first
                    if await done_lbl.count() > 0:
                        val = await done_lbl.locator("xpath=..").inner_text()
                        val = val.replace("Стоимость исполненных обязательств", "").replace("₽", "").strip()
                        results['execution']['accepted'] = val
            except:
                pass

            # Fallback from objects totals
            objects_total = 0.0
            for obj in results.get("objects", []):
                objects_total += clean_number(obj.get("total", ""))
            results["objects_total_clean"] = objects_total

            if results.get("price_clean", 0.0) <= 0 and objects_total > 0:
                results["price"] = f"{objects_total:.2f}"
                results["price_clean"] = objects_total
                results["price_source"] = "objects_sum"
                logging.info(f"Price calculated from objects: {objects_total}")

            # Extract requisites
            results["requisites"] = extract_requisites(requisites_raw_blocks)

            # Hashes for change monitoring
            results["objects_hash"] = stable_hash(results["objects"])
            results["requisites_hash"] = stable_hash(results["requisites"])

            print(json.dumps(results, ensure_ascii=False))

        except Exception as e:
            print(json.dumps({"error": str(e)}))
        finally:
            await browser.close()


if __name__ == '__main__':
    asyncio.run(main())
