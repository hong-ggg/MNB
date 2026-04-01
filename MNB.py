import os
import re
import json
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from playwright.sync_api import sync_playwright

# =========================
# 基本設定
# =========================
TZ = ZoneInfo("Asia/Taipei")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

if not OPENAI_API_KEY:
    raise ValueError("缺少 OPENAI_API_KEY，請在 GitHub Secrets 或環境變數中設定")
if not TELEGRAM_TOKEN:
    raise ValueError("缺少 TELEGRAM_TOKEN，請在 GitHub Secrets 或環境變數中設定")

client = OpenAI(api_key=OPENAI_API_KEY)
HEADERS = {"User-Agent": "Mozilla/5.0"}

SOURCES = {
    "cna": {
        "platform": "中央社",
        "display_name": "中央社國際",
        "list_url": "https://www.cna.com.tw/list/aopl.aspx",
        "base_url": "https://www.cna.com.tw",
    },
    "cnyes": {
        "platform": "鉅亨網",
        "display_name": "鉅亨網宏觀",
        "list_url": "https://news.cnyes.com/news/cat/wd_macro",
        "base_url": "https://news.cnyes.com",
    },
}

DATA_DIR = os.path.join(BASE_DIR, "news_push_data")
ALL_NEWS_DIR = os.path.join(DATA_DIR, "all_news")
TELEGRAM_DIR = os.path.join(DATA_DIR, "telegram")
MASTER_DIR = os.path.join(DATA_DIR, "master")
OFFSET_FILE = os.path.join(TELEGRAM_DIR, "telegram_offset.txt")
SUBSCRIBERS_FILE = os.path.join(TELEGRAM_DIR, "subscribers.xlsx")
MESSAGE_FILE = os.path.join(TELEGRAM_DIR, "message.xlsx")

for path in [DATA_DIR, ALL_NEWS_DIR, TELEGRAM_DIR, MASTER_DIR]:
    os.makedirs(path, exist_ok=True)
for src in SOURCES:
    os.makedirs(os.path.join(ALL_NEWS_DIR, src), exist_ok=True)


# =========================
# 共用工具
# =========================
def now_taipei() -> datetime:
    return datetime.now(TZ)


def today_yyyymmdd() -> str:
    return now_taipei().strftime("%Y%m%d")


def now_str() -> str:
    return now_taipei().strftime("%Y-%m-%d %H:%M:%S")


def normalize_text(text: str) -> str:
    text = (text or "").strip()
    return re.sub(r"\s+", " ", text)


def ensure_parent(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def read_excel_or_empty(path: str, columns: list[str]) -> pd.DataFrame:
    if os.path.exists(path):
        try:
            df = pd.read_excel(path, dtype=str).fillna("")
            for c in columns:
                if c not in df.columns:
                    df[c] = ""
            return df[columns]
        except Exception:
            pass
    return pd.DataFrame(columns=columns)


def get_time_window():
    """
    規則：
    - 週一：抓上週五 06:00 ~ 今日 06:00
    - 週二~週日：抓前一天 06:00 ~ 今日 06:00
    """
    now = now_taipei()
    today_0600 = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now < today_0600:
        today_0600 = today_0600 - timedelta(days=1)

    wd = today_0600.weekday()
    if wd == 0:
        start_dt = today_0600 - timedelta(days=3)
    else:
        start_dt = today_0600 - timedelta(days=1)
    end_dt = today_0600
    return start_dt, end_dt


def save_master(source_key: str, rows: list[dict]):
    path = os.path.join(MASTER_DIR, f"{source_key}_master.xlsx")
    df_new = pd.DataFrame(rows)
    if os.path.exists(path):
        df_old = pd.read_excel(path, dtype=str).fillna("")
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new

    if not df.empty and "url" in df.columns:
        df = df.drop_duplicates(subset=["url"], keep="first")
    df.to_excel(path, index=False)
    return df


def save_all_news_excel(source_key: str, platform_name: str, rows: list[dict]):
    date_str = today_yyyymmdd()
    file_name = f"{date_str} {platform_name}新聞.xlsx"
    path = os.path.join(ALL_NEWS_DIR, source_key, file_name)
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["platform", "title", "time_text", "published_at", "url"])
    df.to_excel(path, index=False)
    print(f"[INFO] 已輸出所有新聞 -> {path}")
    return path


def select_top10(source_key: str, platform_name: str, rows: list[dict]) -> list[dict]:
    if not rows:
        return []

    news = [{"title": r["title"], "time_text": r["time_text"], "url": r["url"]} for r in rows]

    prompt = f"""
                請扮演具備宏觀策略視野的研究員，從以下 {platform_name} 新聞中選出最重要10篇，作為晨會推播。

                【篩選底層邏輯：6+1 動態分析模型】
                請在背景運用此模型判斷當下市場的「主導變數」（例如：若當前戰火猛烈，則「事件面」與「政治面」為主導），並將挑選重心高度集中在能反映主導變數的新聞上：
                一、事件面：戰爭衝突、重大安全事件、天災等具衝擊性的突發狀況。
                二、政治面：大國領袖角力、制裁、貿易戰、重要大選與政局。
                三、政策面：央行貨幣政策（利率/流動性）、財政與產業政策的實際行動。
                四、基本面：最新發布的通膨（能源/商品）、就業、經濟成長關鍵數據。
                五、市場面：股債匯市劇烈異動、關鍵利差變化。
                六、金流面：外資流向、國際收支變化。
                ＋1 隱含約束面：觸及通膨失控、匯率防線、財政失序等市場底線的訊號。

                【具體挑選標準】：
                1. 緊貼主導變數：優先挑選符合當下主導變數的「最新突發事件」、「領袖/官員最新關鍵放話」、「具震撼性的總經數據」。
                2. 拒絕事後諸葛：嚴格排除事後評價、歷史回顧、專欄解析、盤後統整等缺乏「新事實」的觀點文章。
                3. 精準去重：排除完全重複的內容，但若為同一重大事件的「最新實質進展」或「雙方角力對峙」則應保留。

                請直接回傳 JSON array，絕對不要有任何其他文字說明（包含研判過程也請勿寫出），確保格式可直接被程式解析。
                - 每筆格式：
                [
                {{
                    "title": "...",
                    "time_text": "...",
                    "url": "..."
                }}
                ]

                新聞如下：
                {json.dumps(news, ensure_ascii=False)}
                """

    try:
        r = client.responses.create(model="gpt-5.4-nano", input=prompt)
        text = r.output_text.strip()
        return json.loads(text)
    except Exception as e:
        print(f"[WARN] {source_key} GPT 選稿失敗，改用前10篇: {e}")
        return news[:10]


def build_message(platform_name: str, start_dt: datetime, end_dt: datetime, top_rows: list[dict]) -> str:
    lines = [
        f"{platform_name} Top10",
        f"區間：{start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%Y-%m-%d %H:%M')}",
        "",
    ]
    if not top_rows:
        lines.append("本時段沒有符合條件的新聞。")
        return "\n".join(lines)

    for i, row in enumerate(top_rows, 1):
        lines.append(f"{i}. {row['title']}")
        lines.append(str(row.get("time_text", "")))
        lines.append(str(row.get("url", "")))
        lines.append("")
    return "\n".join(lines).strip()


# =========================
# Telegram
# =========================
def load_offset():
    if not os.path.exists(OFFSET_FILE):
        return None
    with open(OFFSET_FILE, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    return int(raw) if raw else None


def save_offset(offset: int):
    with open(OFFSET_FILE, "w", encoding="utf-8") as f:
        f.write(str(offset))


def get_updates():
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {"timeout": 10}
    offset = load_offset()
    if offset is not None:
        params["offset"] = offset

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram getUpdates 失敗: {data}")
    return data.get("result", [])


def update_subscribers():
    columns = [
        "join_date", "join_time", "chat_id", "chat_type", "username",
        "first_name", "last_name", "message_text", "is_new_subscriber"
    ]
    subs_df = read_excel_or_empty(SUBSCRIBERS_FILE, columns)
    updates = get_updates()
    if not updates:
        return subs_df

    existing_ids = set(subs_df["chat_id"].astype(str).tolist()) if not subs_df.empty else set()
    new_rows = []

    for upd in updates:
        msg = upd.get("message") or upd.get("edited_message")
        if not msg:
            continue

        chat = msg.get("chat", {})
        chat_id = str(chat.get("id", "")).strip()
        if not chat_id:
            continue

        text = str(msg.get("text", "")).strip()
        ts = now_taipei()
        is_new = "Y" if chat_id not in existing_ids else "N"

        if is_new == "Y":
            existing_ids.add(chat_id)

        new_rows.append({
            "join_date": ts.strftime("%Y-%m-%d"),
            "join_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "chat_id": chat_id,
            "chat_type": str(chat.get("type", "")),
            "username": str(chat.get("username", "")),
            "first_name": str(chat.get("first_name", "")),
            "last_name": str(chat.get("last_name", "")),
            "message_text": text,
            "is_new_subscriber": is_new,
        })

    if new_rows:
        subs_df = pd.concat([subs_df, pd.DataFrame(new_rows)], ignore_index=True)
        subs_df.to_excel(SUBSCRIBERS_FILE, index=False)

    save_offset(max(u["update_id"] for u in updates) + 1)
    return subs_df


def get_unique_chat_ids_from_subscribers() -> list[str]:
    columns = [
        "join_date", "join_time", "chat_id", "chat_type", "username",
        "first_name", "last_name", "message_text", "is_new_subscriber"
    ]
    subs_df = read_excel_or_empty(SUBSCRIBERS_FILE, columns)
    if subs_df.empty:
        return []
    ids = subs_df["chat_id"].astype(str).str.strip()
    ids = ids[ids != ""]
    return ids.drop_duplicates().tolist()


def send_telegram_message(chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    r = requests.post(url, data=payload, timeout=30)
    r.raise_for_status()


def append_message_log(date_str: str, platform_name: str, message_text: str):
    columns = ["date", "platform", "message"]
    df = read_excel_or_empty(MESSAGE_FILE, columns)
    df = pd.concat([
        df,
        pd.DataFrame([{
            "date": date_str,
            "platform": platform_name,
            "message": message_text,
        }])
    ], ignore_index=True)
    df.to_excel(MESSAGE_FILE, index=False)


def push_to_all_subscribers(platform_name: str, message_text: str):
    chat_ids = get_unique_chat_ids_from_subscribers()
    if not chat_ids:
        print(f"[INFO] {platform_name} 無可推送訂閱者")
        return

    for chat_id in chat_ids:
        try:
            send_telegram_message(chat_id, message_text)
            print(f"[SUCCESS] {platform_name} 已送出 -> {chat_id}")
        except Exception as e:
            print(f"[FAILED] {platform_name} 送出失敗 -> {chat_id}: {e}")


# =========================
# 中央社
# =========================
def crawl_cna(start_dt: datetime, end_dt: datetime) -> list[dict]:
    cfg = SOURCES["cna"]
    results = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(cfg["list_url"], wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2000)

        while True:
            btn = page.locator("#SiteContent_uiViewMoreBtn_Style3")
            if btn.count() == 0:
                break
            try:
                btn.first.click()
                page.wait_for_timeout(1200)
            except Exception:
                break

        html = page.content()
        browser.close()

    soup = BeautifulSoup(html, "html.parser")

    for a in soup.select('a[href^="/news/aopl/"]'):
        href = a.get("href", "")
        url = cfg["base_url"] + href
        if not url or url in seen:
            continue

        title_tag = a.select_one("h2 span") or a.select_one("h2")
        time_tag = a.select_one(".date, .time")
        title = normalize_text(title_tag.get_text(" ", strip=True) if title_tag else "")
        time_text = normalize_text(time_tag.get_text(" ", strip=True) if time_tag else "")

        if not title:
            continue

        published_dt = None
        try:
            published_dt = datetime.strptime(time_text, "%Y/%m/%d %H:%M").replace(tzinfo=TZ)
        except Exception:
            pass

        if published_dt and start_dt <= published_dt <= end_dt:
            results.append({
                "platform": cfg["platform"],
                "title": title,
                "time_text": time_text,
                "published_at": published_dt.isoformat(),
                "url": url,
            })
        seen.add(url)

    results.sort(key=lambda x: x["published_at"], reverse=True)
    return results


# =========================
# 鉅亨網
# =========================
def fetch_html(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"[抓文章失敗] {url} -> {e}")
        return None


def parse_cnyes_article_datetime_and_title(url: str):
    html = fetch_html(url)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    title = None
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        title = normalize_text(og.get("content"))
    if not title and soup.title and soup.title.string:
        title = normalize_text(soup.title.string)
    if not title:
        h1 = soup.select_one("h1")
        if h1:
            title = normalize_text(h1.get_text(" ", strip=True))

    published_dt = None
    tag = soup.select_one("time[datetime]")
    if tag and tag.get("datetime"):
        raw = tag["datetime"].strip()
        try:
            if raw.endswith("Z"):
                raw = raw.replace("Z", "+00:00")
            published_dt = datetime.fromisoformat(raw)
            if published_dt.tzinfo is None:
                published_dt = published_dt.replace(tzinfo=TZ)
            else:
                published_dt = published_dt.astimezone(TZ)
        except Exception:
            published_dt = None

    if published_dt is None:
        html_text = str(soup)
        m = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)', html_text)
        if m:
            try:
                published_dt = datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).astimezone(TZ)
            except Exception:
                published_dt = None

    return published_dt, title


def crawl_cnyes(start_dt: datetime, end_dt: datetime) -> list[dict]:
    cfg = SOURCES["cnyes"]
    results = []
    seen_urls = set()
    too_old_count = 0
    max_scroll_rounds = 200
    no_new_links_rounds = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(cfg["list_url"], wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)

        for scroll_round in range(1, max_scroll_rounds + 1):
            anchors = page.locator("a[href^='/news/id/']")
            count = anchors.count()
            new_links_this_round = []

            for i in range(count):
                try:
                    href = anchors.nth(i).get_attribute("href")
                    if href and re.match(r"^/news/id/\d+$", href):
                        full_url = urljoin(cfg["base_url"], href)
                        if full_url not in seen_urls:
                            seen_urls.add(full_url)
                            new_links_this_round.append(full_url)
                except Exception:
                    continue

            if not new_links_this_round:
                no_new_links_rounds += 1
            else:
                no_new_links_rounds = 0

            for url in new_links_this_round:
                published_dt, title = parse_cnyes_article_datetime_and_title(url)
                if published_dt is None or not title:
                    continue

                if published_dt > end_dt:
                    continue

                if start_dt <= published_dt <= end_dt:
                    results.append({
                        "platform": cfg["platform"],
                        "title": title,
                        "time_text": published_dt.strftime("%Y-%m-%d %H:%M"),
                        "published_at": published_dt.isoformat(),
                        "url": url,
                    })
                    too_old_count = 0
                else:
                    too_old_count += 1
                    if too_old_count >= 10:
                        browser.close()
                        results.sort(key=lambda x: x["published_at"], reverse=True)
                        return results

            if no_new_links_rounds >= 5:
                break

            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(0.5)

        browser.close()

    results.sort(key=lambda x: x["published_at"], reverse=True)
    return results


# =========================
# 主流程
# =========================
def run_for_source(source_key: str, start_dt: datetime, end_dt: datetime):
    cfg = SOURCES[source_key]
    print("=" * 80)
    print(f"[SOURCE] {cfg['display_name']}")
    print(f"[TIME RANGE] {start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)

    if source_key == "cna":
        all_rows = crawl_cna(start_dt, end_dt)
    elif source_key == "cnyes":
        all_rows = crawl_cnyes(start_dt, end_dt)
    else:
        raise ValueError(f"不支援的來源: {source_key}")
    
    save_master(source_key, all_rows)
    save_all_news_excel(source_key, cfg["platform"], all_rows)

    top_rows = select_top10(source_key, cfg["platform"], all_rows)
    message_text = build_message(cfg["platform"], start_dt, end_dt, top_rows)
    append_message_log(today_yyyymmdd(), cfg["platform"], message_text)
    push_to_all_subscribers(cfg["platform"], message_text)


def main():
    print("[STEP 1] 更新 Telegram 訂閱者名單...")
    update_subscribers()

    start_dt, end_dt = get_time_window()

    print("[STEP 2] 分網站抓新聞、輸出 Excel、記錄訊息並推播...")
    for source_key in ["cna", "cnyes"]:
        try:
            run_for_source(source_key, start_dt, end_dt)
        except Exception as e:
            print(f"[ERROR] {source_key} 執行失敗: {e}")

    print("[DONE] 完成")


if __name__ == "__main__":
    main()
