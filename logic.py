import random
import os
import re
import time
from datetime import datetime, date
import urllib.parse
import streamlit as st
from bs4 import BeautifulSoup
import csv
import os.path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 新しいライブラリ ---
from curl_cffi import requests
from fake_useragent import UserAgent

# ==========================================
# 🏪 店舗リスト設定エリア
# ==========================================
STORE_CONFIG = {
    "三ノ輪UNO": {
        "url": "https://min-repo.com/tag/%e4%b8%89%e3%83%8e%e8%bc%aauno/",
        "event_text": "旧イベ: 1のつく日 (1, 11, 21, 31) / ゾロ目"
    },
    "楽園アメ横": {
        "url": "https://min-repo.com/tag/%e6%a5%bd%e5%9c%92%e3%82%a2%e3%83%a1%e6%a8%aa%e5%ba%97/",
        "event_text": "旧イベ: 11日, 22日, 月日ゾロ目の日 / 周年: 1月6日"
    },
    "エスパス上野新館": {
        "url": "https://min-repo.com/tag/%e3%82%a8%e3%82%b9%e3%83%91%e3%82%b9%e6%97%a5%e6%8b%93%e4%b8%8a%e9%87%8e%e6%96%b0%e9%a4%a8/",
        "event_text": "旧イベ: 4のつく日, 7のつく日, 月日ゾロ目の日 / 特日: 14日"
    },
    "エスパス上野本館": {
        "url": "https://min-repo.com/tag/%e3%82%a8%e3%82%b9%e3%83%91%e3%82%b9%e6%97%a5%e6%8b%93%e4%b8%8a%e9%87%8e%e6%9c%ac%e9%a4%a8/",
        "event_text": "月イチ周年日: 21日 / 周年: 8月21日 / 7のつく日 / ゾロ目"
    },
    "ジャラン水元(旧ヴィーナス)": {
        "url": "https://min-repo.com/tag/%e3%83%b4%e3%82%a3%e3%83%bc%e3%83%8a%e3%82%b9%e5%8d%97%e6%b0%b4%e5%85%831%e5%8f%b7%e5%ba%97/",
        "event_text": "旧イベ: 5のつく日, 9のつく日 / 周年: 8月8日"
    },
    "マルハン亀有": {
        "url": "https://min-repo.com/tag/%e3%83%9e%e3%83%ab%e3%83%8f%e3%83%b3%e4%ba%80%e6%9c%89%e5%ba%97/",
        "event_text": "旧イベ: 3,5,7,8の日 / 1,11,14,22日 / 月日ゾロ目"
    },
}

# ==========================================
# 🕒 収集の安全時間帯ガード
# ==========================================
SCRAPE_SAFE_START = (8, 0)   # 08:00
SCRAPE_SAFE_END   = (9, 59)  # 09:59

def is_safe_scrape_time(now_dt=None):
    if now_dt is None: now_dt = datetime.now()
    h, m = now_dt.hour, now_dt.minute
    start_h, start_m = SCRAPE_SAFE_START
    end_h, end_m     = SCRAPE_SAFE_END
    current_mins = h * 60 + m
    start_mins = start_h * 60 + start_m
    end_mins = end_h * 60 + end_m
    return start_mins <= current_mins <= end_mins

def safe_window_text():
    return f"{SCRAPE_SAFE_START[0]:02d}:{SCRAPE_SAFE_START[1]:02d}〜{SCRAPE_SAFE_END[0]:02d}:{SCRAPE_SAFE_END[1]:02d}"

# ==========================================
# 🎯 詳細データを取得する機種リスト
# ==========================================
TARGET_KEYWORDS = [
    "マイジャグ", "マイジャグV", "マイジャグラー",
    "ファンキー", "ファンキージャグラー",
    "アイム", "アイムジャグラー", "ゴージャグ", "ゴーゴージャグラー",
    "ハッピー", "ハッピージャグラー",
    "ディスク", "DISK", "ディスクアップ",
    "北斗", "北斗の拳", "スマスロ北斗"
]

PROXY_LIST_FILE = 'proxy_list.txt'

# ==========================================
# 🛡️ 接続ロジック (最強偽装)
# ==========================================
def get_random_ua():
    try: return UserAgent().random
    except: return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

def load_proxies(filename=PROXY_LIST_FILE):
    if not os.path.exists(filename): return []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except: return []

def get_soup(url, max_retries=3):
    proxies_list = load_proxies()
    attempt_methods = [{"proxy": None, "type": "Direct"}]
    if proxies_list:
        sample = random.sample(proxies_list, min(len(proxies_list), 5))
        for p in sample: attempt_methods.append({"proxy": p, "type": "Proxy"})

    for attempt in attempt_methods:
        time.sleep(random.uniform(1.5, 4.0)) 
        headers = {
            "User-Agent": get_random_ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"'
        }
        proxies_dict = {"http": f"http://{attempt['proxy']}", "https": f"http://{attempt['proxy']}"} if attempt['proxy'] else None

        try:
            print(f"[{attempt['type']}] Fetching: {url[:40]}...")
            response = requests.get(url, headers=headers, proxies=proxies_dict, impersonate="chrome124", timeout=20)
            if response.status_code == 200:
                if "Just a moment" in response.text or "Cloudflare" in response.text:
                    print(f"⚠️ Cloudflare Block detected ({url})")
                    continue
                return BeautifulSoup(response.text, 'html.parser')
            else:
                print(f"⚠️ Status Code: {response.status_code} ({url})")
        except Exception as e:
            print(f"⚠️ Connection Error: {e}")
            continue
    return None

# ==========================================
# データ解析・保存ロジック
# ==========================================
def fetch_machine_detail(url):
    soup = get_soup(url)
    if not soup: return {}
    detail_map = {}
    tables = soup.find_all('table')
    for table in tables:
        headers = [th.get_text(strip=True) for th in table.find_all('th')]
        if 'BB' in headers and 'RB' in headers:
            try:
                idx_num = next((i for i, h in enumerate(headers) if '台番' in h), -1)
                idx_bb = headers.index('BB')
                idx_rb = headers.index('RB')
                idx_total = next((i for i, h in enumerate(headers) if '合' in h), -1)
                if idx_num == -1: continue
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    cols_text = [ele.get_text(strip=True) for ele in cols]
                    if len(cols_text) > max(idx_num, idx_bb, idx_rb):
                        num = cols_text[idx_num]
                        if re.search(r'\d+', num):
                            detail_map[num] = {'BB': cols_text[idx_bb], 'RB': cols_text[idx_rb], '合成': cols_text[idx_total] if idx_total != -1 else '-'}
            except: continue
            break
    return detail_map

def process_extra_data(target_machines):
    extra_data_map = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_url = {executor.submit(fetch_machine_detail, m_url): m_name for m_name, m_url in target_machines}
        for future in as_completed(future_to_url):
            try: extra_data_map.update(future.result())
            except: pass
    return extra_data_map

def save_daily_data(detail_url, date_str, save_dir):
    # ▼【安全対策】もし処理開始時点で時間が過ぎていたら、スキップする (未実行タスクのキャンセル)
    if not is_safe_scrape_time():
        return False
    
    # ここまで到達できた＝実行許可が出たタスクなので、以降は最後まで処理を完遂させる
    if not os.path.exists(save_dir): os.makedirs(save_dir)
    filename = os.path.join(save_dir, f"{date_str}.csv")
    if os.path.exists(filename): return "EXIST"
    if not detail_url.startswith("http"): detail_url = "https://min-repo.com" + detail_url
    target_url = f"{detail_url}?kishu=all"

    soup = get_soup(target_url)
    if not soup: return False

    tables = soup.find_all('table')
    if not tables: return False
    main_data = []
    machine_link_map = {}
    headers = None

    for table in tables:
        header_row = table.find('tr')
        if not header_row: continue
        tmp_headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        if not all(r in str(tmp_headers) for r in ['機種', '台番', '差枚']): continue
        headers = tmp_headers
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all(['td', 'th'])
            if not cols: continue
            cols_text = [ele.get_text(strip=True) for ele in cols]
            if cols_text == tmp_headers: continue
            link_ele = cols[0].find('a')
            if link_ele:
                m_name = cols_text[0] if len(cols_text) > 0 else ""
                full_link = urllib.parse.urljoin(target_url, link_ele.get('href'))
                if m_name: machine_link_map[m_name] = full_link
            main_data.append(cols_text)
        if main_data: break

    if not main_data: return False

    target_machines = []
    for m_name, m_url in machine_link_map.items():
        for kw in TARGET_KEYWORDS:
            if kw in m_name:
                target_machines.append((m_name, m_url))
                break
    extra_data_map = process_extra_data(target_machines) if target_machines else {}

    output_headers = headers + ['BB', 'RB', '合成']
    output_rows = [output_headers]
    try: idx_num = next(i for i, h in enumerate(headers) if '台番' in h)
    except: idx_num = 1

    for row in main_data:
        if len(row) > idx_num:
            num = row[idx_num]
            clean_num = re.sub(r'\D', '', str(num))
            if num in extra_data_map: ex = extra_data_map[num]; row_extended = row + [ex['BB'], ex['RB'], ex['合成']]
            elif clean_num in extra_data_map: ex = extra_data_map[clean_num]; row_extended = row + [ex['BB'], ex['RB'], ex['合成']]
            else: row_extended = row + ['-', '-', '-']
            output_rows.append(row_extended)
        else: output_rows.append(row + ['-', '-', '-'])

    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerows(output_rows)
        return True
    except: return False

# ==========================================
# 🚀 実行エントリーポイント
# ==========================================
def run_scraping(store_name, start_date, end_date, max_workers=3): 
    store_info = STORE_CONFIG.get(store_name)
    if not store_info: return
    save_dir = store_name 
    if not os.path.exists(save_dir): os.makedirs(save_dir)

    status_text = st.empty()
    progress_bar = st.progress(0)
    status_text.info(f"🚀 {store_name} のデータを収集中...")

    if not is_safe_scrape_time():
        status_text.error(f"⛔ 時間外です ({safe_window_text()})。9:59を過ぎたため収集を開始できません。")
        progress_bar.empty()
        return

    current_url = store_info["url"]
    current_year = datetime.now().year
    last_month = 13
    target_tasks = [] 
    page_count = 1
    max_scan_pages = 25 

    while page_count <= max_scan_pages:
        if not is_safe_scrape_time():
            status_text.warning(f"⏰ 時間オーバー ({safe_window_text()})！ 進行中の処理が完了次第、停止します...")
            break

        status_text.write(f"🔍 {store_name} リンク探索中... {page_count}ページ目")
        soup = get_soup(current_url)
        if not soup: break
        all_links = soup.find_all('a')
        
        for link in all_links:
            text = link.get_text(strip=True)
            href = link.get('href')
            if not href: continue
            match = re.search(r'(\d{1,2})/(\d{1,2})', text)
            if match and re.search(r'\d+', href):
                m, d = int(match.group(1)), int(match.group(2))
                if not (1 <= m <= 12): continue
                if page_count > 1 and m > last_month and (last_month != 13): current_year -= 1
                last_month = m
                try: d_date = date(current_year, m, d)
                except: continue
                if d_date > end_date: continue 
                if d_date < start_date:
                    if (start_date - d_date).days > 7: page_count = 999; break
                    continue
                date_str = f"{current_year}-{m:02d}-{d:02d}"
                if not any(t[1] == date_str for t in target_tasks): target_tasks.append((href, date_str))

        if page_count == 999: break
        next_page = soup.find('a', class_='next') or soup.find('a', string=re.compile(r'次|Next|next', re.I))
        if next_page and next_page.get('href'): current_url = next_page.get('href'); page_count += 1; time.sleep(1) 
        else: break

    total_tasks = len(target_tasks)
    if total_tasks == 0 and page_count != 999 and is_safe_scrape_time():
        status_text.warning(f"⚠️ {store_name}: データが見つかりませんでした")
        return

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_date = {executor.submit(save_daily_data, href, d_str, save_dir): d_str for href, d_str in target_tasks}
        for future in as_completed(future_to_date):
            # メインループ側でbreakするとwaitしてしまうが、
            # ここでは「完了したやつを受け取る」だけなのでループを回し続ける。
            # 時間外になったら save_daily_data の冒頭で False が返ってくるので、一瞬で消化される。
            d_str = future_to_date[future]
            try:
                res = future.result()
                if res: # Trueなら成功、Falseならスキップまたは失敗
                    completed += 1
                prog = int((completed / total_tasks) * 100)
                progress_bar.progress(prog)
            except: pass

    if completed > 0:
        status_text.success(f"🎉 {store_name}: 完了 ({completed}/{total_tasks})")
    elif not is_safe_scrape_time():
        status_text.warning("⏰ 時間切れのため、未実行のタスクはスキップされました。")
    
    time.sleep(1)
    status_text.empty()
    progress_bar.empty()