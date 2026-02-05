#!/usr/bin/env python3
import sys
import os
import json
import re
import asyncio
from urllib.parse import urljoin, urlparse, unquote
from playwright.async_api import async_playwright

ALLOWED_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".xml", ".html", ".htm", ".zip", ".rar"
}

FILestore_HINTS = [
    "/filestore/public/",
    "/filestore/",
    "download/rgk2/file.html?uid=",
]


def is_attachment_url(href: str) -> bool:
    if not href:
        return False
    lower = href.lower()
    if any(hint in lower for hint in FILestore_HINTS):
        return True
    if "attachmentid=" in lower:
        return True
    if "/download/" in lower:
        return True
    # Fallback: only accept direct file URLs from filestore
    if "/filestore/" in lower and any(lower.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return True
    return False


def sanitize_filename(name: str) -> str:
    name = name.strip().replace("\n", " ")
    name = re.sub(r"\s+", " ", name)
    # Replace filesystem-unfriendly characters
    name = re.sub(r"[^A-Za-z0-9_.() -]", "_", name)
    if not name:
        return "file"
    return name


def guess_filename(href: str, text: str, idx: int) -> str:
    path = urlparse(href).path
    base = os.path.basename(path)
    if base:
        return sanitize_filename(base)
    if text:
        return sanitize_filename(text) + f"_{idx}"
    return f"file_{idx}"


def filename_from_headers(headers):
    # Content-Disposition: attachment; filename="file.pdf"
    cd = headers.get("content-disposition") or headers.get("Content-Disposition")
    if not cd:
        return None
    match = re.search(r'filename\\*=UTF-8\\\'\\\'([^;]+)', cd)
    if match:
        return sanitize_filename(unquote(match.group(1)))
    match = re.search(r'filename=\"?([^\";]+)\"?', cd)
    if match:
        return sanitize_filename(unquote(match.group(1)))
    return None


def extension_from_content_type(headers):
    ct = headers.get("content-type") or headers.get("Content-Type") or ""
    ct = ct.split(";")[0].strip().lower()
    mapping = {
        "application/pdf": ".pdf",
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
        "application/zip": ".zip",
        "application/xml": ".xml",
        "text/xml": ".xml",
        "text/html": ".html",
    }
    return mapping.get(ct, "")


def normalize_url(href: str, base_url: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(base_url, href)


async def collect_links(page, url):
    await page.goto(url, timeout=60000, wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)
    anchors = page.locator("a")
    count = await anchors.count()
    links = []
    for i in range(count):
        a = anchors.nth(i)
        href = await a.get_attribute("href")
        text = ""
        try:
            text = (await a.inner_text()) if await a.count() > 0 else ""
        except Exception:
            text = ""
        if href:
            links.append((href, text))
    return links

async def collect_attachment_ids(page):
    html = await page.content()
    ids = re.findall(r"attachmentId=(\\d+)", html)
    return list(dict.fromkeys(ids))

async def collect_filestore_links(page):
    html = await page.content()
    links = re.findall(r"https?://[^\\\"'\\s]+/filestore/public/[^\\\"'\\s]+", html)
    return list(dict.fromkeys(links))


async def main():
    if len(sys.argv) < 3:
        print(json.dumps({"error": "Usage: fetch_contract_attachments.py <reestrNumber> <base_url>"}))
        sys.exit(1)

    reestr_number = sys.argv[1]
    base_url = sys.argv[2]

    download_dir = os.path.join("attachments", reestr_number)
    os.makedirs(download_dir, exist_ok=True)

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            base_url = f"https://zakupki.gov.ru/epz/contract/contractCard/common-info.html?reestrNumber={reestr_number}"
            await page.goto(base_url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            base_html = await page.content()

            m = re.search(r"contractInfoId=(\\d+)", base_html)
            contract_info_id = m.group(1) if m else None
            if contract_info_id:
                doc_url = f"https://zakupki.gov.ru/epz/contract/contractCard/document-info.html?reestrNumber={reestr_number}&contractInfoId={contract_info_id}"
            else:
                doc_url = f"https://zakupki.gov.ru/epz/contract/contractCard/document-info.html?reestrNumber={reestr_number}"
            await page.goto(doc_url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(2000)
            doc_html = await page.content()

            filestore_links = re.findall(r"https?://[^\\\"'\\s]+/filestore/public/[^\\\"'\\s]+", doc_html)
            filestore_links = list(dict.fromkeys(filestore_links))

            seen = set()
            for m_url in filestore_links:
                if m_url in seen:
                    continue
                seen.add(m_url)
                uid_match = re.search(r"uid=([A-Fa-f0-9]+)", m_url)
                if uid_match:
                    filename = f"{uid_match.group(1)}"
                else:
                    filename = guess_filename(m_url, "", 0)
                path = os.path.join(download_dir, filename)
                try:
                    resp = None
                    for attempt in range(3):
                        resp = await context.request.get(m_url)
                        if resp.status == 429:
                            await page.wait_for_timeout(1500 * (attempt + 1))
                            continue
                        break
                    if not resp or resp.status != 200:
                        results.append({"url": m_url, "status": resp.status if resp else None, "error": "download_failed"})
                        continue
                    headers = resp.headers
                    real_name = filename_from_headers(headers)
                    if real_name:
                        filename = real_name
                        path = os.path.join(download_dir, filename)
                    else:
                        ext = extension_from_content_type(headers)
                        if ext and not filename.endswith(ext):
                            filename = f"{filename}{ext}"
                            path = os.path.join(download_dir, filename)
                    data = await resp.body()
                    with open(path, "wb") as f:
                        f.write(data)
                    results.append({
                        "url": m_url,
                        "file_name": filename,
                        "path": path,
                        "size": len(data),
                        "status": 200
                    })
                except Exception as e:
                    results.append({"url": m_url, "error": str(e)})

        finally:
            await browser.close()

    print(json.dumps({"reestr_number": reestr_number, "files": results}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
