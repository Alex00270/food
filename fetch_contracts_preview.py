import sys
import asyncio
import json
import re
from playwright.async_api import async_playwright

async def fetch_contract_preview(contract_number):
    """
    Fetches basic preview information for a single contract.
    """
    url = f"https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber={contract_number}"
    
    result = {
        "number": contract_number,
        "year": None,
        "customer": None,
        "price": None,
        "status": "unknown"
    }
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            
            # Extract customer name
            try:
                customer_element = await page.wait_for_selector('[data-bind="text: customer.fullName"]', timeout=10000)
                if customer_element:
                    customer = await customer_element.inner_text()
                    result["customer"] = customer.strip()
            except:
                try:
                    # Alternative selector
                    customer_element = await page.query_selector('.customer-name, .customer-block__name')
                    if customer_element:
                        customer = await customer_element.inner_text()
                        result["customer"] = customer.strip()
                except:
                    pass
            
            # Extract price
            try:
                price_element = await page.wait_for_selector('[data-bind="text: price"]', timeout=10000)
                if price_element:
                    price = await price_element.inner_text()
                    result["price"] = price.strip()
            except:
                try:
                    # Alternative selector
                    price_element = await page.query_selector('.price-block__value, .contract-price')
                    if price_element:
                        price = await price_element.inner_text()
                        result["price"] = price.strip()
                except:
                    pass
            
            # Extract dates to determine year
            try:
                date_element = await page.wait_for_selector('[data-bind="text: contractSigningDate"]', timeout=10000)
                if date_element:
                    date_text = await date_element.inner_text()
                    year_match = re.search(r'(\d{4})', date_text)
                    if year_match:
                        result["year"] = year_match.group(1)
            except:
                try:
                    # Alternative selectors for dates
                    date_selectors = [
                        '[data-bind="text: executionDateStart"]',
                        '[data-bind="text: executionDateEnd"]',
                        '.date-block__start-date',
                        '.date-block__end-date'
                    ]
                    
                    for selector in date_selectors:
                        try:
                            date_element = await page.query_selector(selector)
                            if date_element:
                                date_text = await date_element.inner_text()
                                year_match = re.search(r'(\d{4})', date_text)
                                if year_match:
                                    result["year"] = year_match.group(1)
                                    break
                        except:
                            continue
                except:
                    pass
            
            # Fallback year extraction from contract number
            if not result["year"] and len(contract_number) >= 19:
                potential_year = "20" + contract_number[14:16]
                if 2020 <= int(potential_year) <= 2030:
                    result["year"] = potential_year
                else:
                    result["year"] = "2025"  # Default year
            
            result["status"] = "found"
            
        except Exception as e:
            result["status"] = f"error: {str(e)}"
            
        finally:
            await browser.close()
    
    return result

async def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "No contract numbers provided"}))
        sys.exit(1)
    
    numbers_str = sys.argv[1]
    contract_numbers = [num.strip() for num in numbers_str.split(',') if num.strip()]
    
    if not contract_numbers:
        print(json.dumps({"error": "No valid contract numbers found"}))
        sys.exit(1)
    
    print(f"Fetching preview for {len(contract_numbers)} contracts...", file=sys.stderr)
    
    results = []
    
    for contract_number in contract_numbers:
        if len(contract_number) < 19:
            results.append({
                "number": contract_number,
                "status": "invalid: too short"
            })
            continue
            
        try:
            preview = await fetch_contract_preview(contract_number)
            results.append(preview)
            
            # Small delay between requests to be respectful
            await asyncio.sleep(1)
            
        except Exception as e:
            results.append({
                "number": contract_number,
                "status": f"error: {str(e)}"
            })
    
    print(json.dumps(results, ensure_ascii=False))

if __name__ == '__main__':
    asyncio.run(main())