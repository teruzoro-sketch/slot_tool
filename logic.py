import os
import re
import time
import random
import sqlite3
import urllib.parse
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit as st
from bs4 import BeautifulSoup

# --- 対策ライブラリ ---
try:
    from curl_cffi import requests
    from fake_useragent import UserAgent
except ImportError:
    import requests
    class UserAgent:
        def __init__(self): self.random = "Mozilla/5.0"

# ==========================================
# ⚙️ 設定
# ==========================================
STORE_CONFIG_FILE = "stores.txt"
DB_FILE = "slot_data.db"

# 基本的な時間制限 (8:00〜9:59)
SCRAPE_SAFE_START = (8, 0)
SCRAPE_SAFE_END   = (9, 59)

TARGET_KEYWORDS = [
    "ジャグラー", "マイジャグ", "ファンキー", "アイム", "ゴージャグ", 
    "ハッピー", "ハナハナ", "北斗", "カバネリ", "ディスクアップ"
]

def load_store_config():
    config = {}
    if not os.path.exists(STORE_CONFIG_FILE): return {}
    try:
        with open(STORE_CONFIG_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip() or line.startswith("#"): continue
                parts = line.strip().split("|")
                if len(parts) >= 2:
                    config[parts[0].strip()] = {"url": parts[1].strip(), "event_text": parts[2].strip() if len(parts)>2 else ""}
    except: pass
    return config

STORE_CONFIG = load_store_config()

def is_safe_scrape_time(now_dt=None):
    # ★実験用 (return True) を消して、以下のコードに戻す
    if now_dt is None: now_dt = datetime.now()
    cur = now_dt.hour * 60 + now_dt.minute
    start = SCRAPE_SAFE_START[0] * 60 + SCRAPE_SAFE_START[1]
    end = SCRAPE_SAFE_END[0] * 60 + SCRAPE_SAFE_END[1]
    return start <= cur <= end

def get_soup(url):
    try:
        ua = UserAgent().random
        headers = {"User-Agent": ua, "Referer": "https://www.google.com/"}
        time.sleep(random.uniform(1.5, 3.0))
        resp = requests.get(url, headers=headers, impersonate="chrome124", timeout=15)
        if resp.status_code == 200:
            # .contentを使って文字化け回避
            return BeautifulSoup(resp.content, 'html.parser')
    except Exception as e:
        print(f"Connection Error: {e}")
    return None

def fetch_machine_detail(url):
    soup = get_soup(url)
    if not soup: return {}
    data = {}
    for tbl in soup.find_all('table'):
        ths = [th.get_text(strip=True) for th in tbl.find_all('th')]
        if 'BB' in ths and 'RB' in ths:
            try:
                idx_num = next(i for i, h in enumerate(ths) if '台番' in h)
                idx_bb = ths.index('BB')
                idx_rb = ths.index('RB')
                idx_tot = next((i for i, h in enumerate(ths) if '合' in h), -1)
                for tr in tbl.find_all('tr'):
                    tds = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
                    if len(tds) > max(idx_num, idx_bb, idx_rb) and re.search(r'\d+', tds[idx_num]):
                        data[tds[idx_num]] = {'BB': tds[idx_bb], 'RB': tds[idx_rb], '合成': tds[idx_tot] if idx_tot!=-1 else '-'}
            except: pass
    return data

def process_extra_data(targets, max_workers=3):
    res = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_machine_detail, u): n for n, u in targets}
        for f in as_completed(futures):
            try: res.update(f.result())
            except: pass
    return res

def save_daily_data(detail_url, date_str, store_name):
    if not is_safe_scrape_time(): return False
    
    if not detail_url.startswith("http"): detail_url = "https://min-repo.com" + detail_url
    url = f"{detail_url}?kishu=all"
    soup = get_soup(url)
    if not soup: return False

    tables = soup.find_all('table')
    main_data = []
    link_map = {}
    headers = []

    for tbl in tables:
        h_row = tbl.find('tr')
        if not h_row: continue
        hs = [t.get_text(strip=True) for t in h_row.find_all(['th','td'])]
        if not all(k in str(hs) for k in ['機種','台番','差枚']): continue
        headers = hs
        for tr in tbl.find_all('tr'):
            cols = tr.find_all(['td','th'])
            if not cols: continue
            txts = [c.get_text(strip=True) for c in cols]
            if txts == headers: continue
            
            link = cols[0].find('a')
            if link:
                full = urllib.parse.urljoin(url, link.get('href'))
                link_map[txts[0]] = full
            main_data.append(txts)
        if main_data: break

    targets = [(n, u) for n, u in link_map.items() if any(k in n for k in TARGET_KEYWORDS)]
    extras = process_extra_data(targets) if targets else {}

    # DB用データ構築
    db_rows = []
    try: idx_num = next(i for i, h in enumerate(headers) if '台番' in h)
    except: idx_num = 1
    
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for r in main_data:
        if len(r) <= idx_num: continue
        num = r[idx_num]
        clean_num = re.sub(r'\D', '', str(num))
        ex = extras.get(num) or extras.get(clean_num) or {'BB': '-', 'RB': '-', '合成': '-'}
        
        def clean_int(val):
            return int(re.sub(r'[^\d\-]', '', str(val))) if re.search(r'\d', str(val)) else 0

        diff = clean_int(r[headers.index('差枚')]) if '差枚' in headers else 0
        games = clean_int(r[headers.index('G数')]) if 'G数' in headers else 0
        
        db_rows.append({
            'store_name': store_name,
            'date': date_str,
            'machine_name': r[0],
            'machine_num': clean_int(num),
            'diff_coins': diff,
            'game_count': games,
            'bb_count': clean_int(ex['BB']),
            'rb_count': clean_int(ex['RB']),
            'total_prob': ex['合成'],
            'updated_at': now_str
        })

    # ==========================================
    # 🛡️ ダミーデータ判定 (バリデーション)
    # ==========================================
    if not db_rows: return False

    # 条件: データ数が10件以上あるのに、マイナス差枚が1件もない場合 → ダミーとみなす
    negative_count = sum(1 for row in db_rows if row['diff_coins'] < 0)
    
    if len(db_rows) >= 10 and negative_count == 0:
        print(f"⚠️ 警告: {date_str} のデータはダミーの可能性があります (マイナス差枚なし)。保存をスキップします。")
        return False
    # ==========================================

    # SQLite保存
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("DELETE FROM machine_data WHERE store_name=? AND date=?", (store_name, date_str))
        cur.executemany("""
            INSERT INTO machine_data (store_name, date, machine_name, machine_num, diff_coins, game_count, bb_count, rb_count, total_prob, updated_at)
            VALUES (:store_name, :date, :machine_name, :machine_num, :diff_coins, :game_count, :bb_count, :rb_count, :total_prob, :updated_at)
        """, db_rows)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"DB Error: {e}")
        return False

def run_scraping(store_name, s_date, e_date, workers=3):
    if not is_safe_scrape_time(): return
    info = STORE_CONFIG.get(store_name)
    if not info: return

    soup = get_soup(info['url'])
    if not soup: return
    
    tasks = []
    for a in soup.find_all('a'):
        href = a.get('href')
        txt = a.get_text(strip=True)
        m = re.search(r'(\d{1,2})/(\d{1,2})', txt)
        if m and href:
            mon, day = int(m.group(1)), int(m.group(2))
            try:
                y = s_date.year
                d_obj = date(y, mon, day)
                if d_obj > date.today(): d_obj = date(y-1, mon, day)
                
                if s_date <= d_obj <= e_date:
                    tasks.append((href, f"{d_obj.year}-{mon:02d}-{day:02d}"))
            except: pass

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(save_daily_data, h, d, store_name): d for h, d in tasks}
        for f in as_completed(futs):
            try: f.result()
            except: pass