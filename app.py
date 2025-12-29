import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import glob
import os
import re
import json
import csv
import time
from datetime import datetime, timedelta

import logic

# ==========================================
# 設定・定数エリア
# ==========================================
st.set_page_config(page_title="Slot Master Pro", layout="wide", page_icon="🎰")
pd.set_option("styler.render.max_elements", 1000000)

MEMO_FILE = "daily_memos.json"

# ▼ 【2025年12月版】メーカー・グループ辞書
MAKER_DICT = {
    "🤡 北電子 (ジャグラー)": ["ジャグラー", "マイジャグ", "ファンキー", "ハッピー", "アイム", "ゴージャグ", "ミスター", "ガールズ", "ダンまち", "グランベルム"],
    "👽 Sammy系": ["北斗", "カバネリ", "防振り", "エウレカ", "ゴールデンカムイ", "コードギアス", "幼女戦記", "頭文字D", "傷物語", "バイオハザード RE:2", "ディスクアップ", "ガメラ", "アラジン", "ファイヤードリフト", "東京リベンジャーズ", "A-SLOT", "鬼武者3"],
    "🤖 SANKYO系": ["ヴァルヴレイヴ", "ヴヴヴ", "からくり", "シンフォギア", "炎炎", "マクロス", "ユニコーン", "かぐや様", "エヴァ", "ゴジラ", "アクエリオン", "ガンダム", "アイドルマスター"],
    "⚡ ユニバ系": ["沖ドキ", "天膳", "バジリスク", "まどか", "ハーデス", "花火", "ハナビ", "バーサス", "アクロス", "サンダー", "ファミスタ", "ワードオブライツ", "クランキー", "緑ドン", "桃太郎電鉄"],
    "🐼 大都系": ["番長", "リゼロ", "鏡", "吉宗", "アオハル", "SAO", "ソードアート", "冴えない", "クレア", "秘宝伝", "政宗", "忍魂", "ゾンビランドサガ"],
    "🐒 山佐系": ["モンキーターン", "ゴッドイーター", "パルサー", "転スラ", "ナイツ", "キン肉マン", "ウィッチ", "ゼーガペイン", "ネオプラネット", "ハイパーラッシュ"],
    "🕊️ オリンピア/平和": ["ToLOVEる", "トラブル", "戦国乙女", "主役は銭形", "麻雀格闘", "ルパン", "ガルパン", "黄門ちゃま", "バキ", "刃牙", "ラブ嬢", "バンドリ"],
    "🐺 カプコン系": ["鬼武者", "バイオハザード", "モンハン", "モンスターハンター", "デビルメイクライ", "ストリートファイター"],
    "👻 藤商事系": ["禁書目録", "インデックス", "リング", "地獄少女", "フェアリーテイル", "アリア", "ゴブリンスレイヤー", "超電磁砲", "レールガン"],
    "🌺 パイオニア": ["ハナハナ", "オアシス", "シオサイ"],
    "🐉 コナミ": ["マジカルハロウィン", "マジハロ", "ボンバーガール", "戦国コレクション", "戦コレ", "G1優駿", "防空少女", "サイレントヒル"],
    "🍑 ネット/カルミナ": ["チバリヨ", "十字架", "シンデレラブレイド", "スナイパイ", "賞金首", "ミルキィホームズ", "プリズムナナ"],
    "🐈 オーイズミ": ["ひぐらし", "オーバーロード", "1000ちゃん", "閃乱カグラ"],
    "🔔 その他": ["ビンゴ", "ジャックポット", "ウルトラマン", "ワンパンマン", "リコリス"]
}

# --- ブドウ逆算ロジック (v2) ---
def calc_grape_prob_v2(row):
    specs = {
        "マイジャグ": {"bb": 240, "rb": 96, "grape_pay": 8, "cherry_pay": 2, "replay": 7.298, "cherry": 36.0},
        "ファンキー": {"bb": 240, "rb": 96, "grape_pay": 8, "cherry_pay": 2, "replay": 7.3,  "cherry": 36.0},
        "アイム":     {"bb": 252, "rb": 96, "grape_pay": 8, "cherry_pay": 4, "replay": 7.3,  "cherry": 33.0},
        "ハッピー":   {"bb": 240, "rb": 96, "grape_pay": 8, "cherry_pay": 2, "replay": 7.3,  "cherry": 36.0},
        "ゴージャグ": {"bb": 240, "rb": 96, "grape_pay": 8, "cherry_pay": 2, "replay": 7.3,  "cherry": 33.0},
        "ガールズ":   {"bb": 240, "rb": 96, "grape_pay": 8, "cherry_pay": 2, "replay": 7.3,  "cherry": 36.0},
        "ミスター":   {"bb": 240, "rb": 96, "grape_pay": 8, "cherry_pay": 2, "replay": 7.3,  "cherry": 36.0},
        "ミラクル":   {"bb": 240, "rb": 96, "grape_pay": 8, "cherry_pay": 2, "replay": 7.3,  "cherry": 36.0},
    }
    
    target_spec = None
    for k, v in specs.items():
        if k in str(row['機種']): target_spec = v; break
    
    if not target_spec or row['G数'] < 500: return 0.0

    g = row['G数']; diff = row['差枚']; bb = row['BB']; rb = row['RB']
    s = target_spec
    
    bonus_net = (bb * s['bb']) + (rb * s['rb'])
    est_cherry_count = g / s['cherry']
    cherry_pay_total = est_cherry_count * s['cherry_pay']
    est_replay_count = g / s['replay']
    normal_in = (g * 3) - (est_replay_count * 3)
    grape_pay_total = diff - bonus_net - cherry_pay_total + normal_in
    
    if grape_pay_total > 0:
        est_grape_count = grape_pay_total / s['grape_pay']
        if est_grape_count > 0: return g / est_grape_count
    return 0.0

def calc_grape_prob(row): return calc_grape_prob_v2(row)

def detect_maker(model_name):
    for maker, keywords in MAKER_DICT.items():
        for kw in keywords:
            if kw in model_name: return maker
    return "その他"

st.markdown("""
    <style>
        .main .block-container { max-width: 100% !important; padding: 1rem 1rem 3rem 1rem !important; }
        .custom-table { width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
        .custom-table th { background-color: #f8f9fa; padding: 12px 8px; text-align: center; border: 1px solid #dee2e6; font-weight: bold; color: #495057; }
        .custom-table td { padding: 12px 10px; border: 1px solid #dee2e6; vertical-align: top; background-color: #fff; line-height: 1.6; color: #333; }
        .td-date   { width: 12%; text-align: center; font-weight: bold; white-space: nowrap; color: #333; }
        .td-total  { width: 10%; text-align: right; font-weight: bold; font-size: 15px; color: #333; }
        .td-avg    { width: 8%; text-align: right; font-weight: bold; color: #333; }
        .td-g      { width: 10%; text-align: right; color: #666; font-size: 13px; }
        .td-end    { width: 12%; text-align: center; font-weight: bold; color: #d63384; } 
        .td-models { width: 48%; text-align: left; font-size: 13px; color: #333; }
        .val-plus { color: #d32f2f !important; }
        .val-minus { color: #333 !important; }
        .model-line { display: block; margin-bottom: 4px; border-bottom: 1px dashed #eee; padding-bottom: 2px; }
        .memo-item { display: block; color: #0d6efd; font-weight: bold; margin-bottom: 6px; background-color: #e7f1ff; padding: 4px 8px; border-radius: 4px; }
        .icon-star { color: #ff9800; font-weight: bold; font-size: 1.1em; } 
        .icon-double { color: #e91e63; font-weight: bold; font-size: 1.1em; } 
        .icon-circle { color: #4caf50; font-weight: bold; } 
        .icon-spin { color: #6610f2; font-weight: bold; font-size: 1.1em; }
        .analysis-box { background-color: #e3f2fd; border: 1px solid #90caf9; border-radius: 8px; padding: 15px; margin-top: 20px; }
        .pagination-box { text-align: center; padding: 10px; background: #f0f2f6; border-radius: 10px; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

def load_memos():
    if os.path.exists(MEMO_FILE):
        with open(MEMO_FILE, "r", encoding="utf-8") as f:
            try: return json.load(f)
            except: return {}
    return {}

def save_memo(date_str, text, store_name):
    memos = load_memos()
    key = f"{store_name}_{date_str}"
    memos[key] = text
    with open(MEMO_FILE, "w", encoding="utf-8") as f:
        json.dump(memos, f, ensure_ascii=False, indent=4)

@st.cache_data
def load_and_process_data(folder_path):
    if not os.path.exists(folder_path): return pd.DataFrame()
    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not all_files: return pd.DataFrame()
    df_list = []
    for f in all_files:
        try:
            date_str = os.path.basename(f).replace(".csv", "")
            temp_df = pd.read_csv(f, encoding='utf-8-sig')
            temp_df['日付'] = pd.to_datetime(date_str)
            df_list.append(temp_df)
        except: continue
    if not df_list: return pd.DataFrame()
    df = pd.concat(df_list, ignore_index=True)
    if not df.empty: df = df.drop_duplicates(subset=['日付', '台番'], keep='last')
    
    cols_to_num = ['台番', '差枚', 'G数', 'BB', 'RB', '合成']
    df.columns = [str(c).strip().replace('\ufeff', '') for c in df.columns]
    for col in cols_to_num:
        if col in df.columns:
            s_raw = df[col].astype(str).str.strip()
            def safe_convert(val):
                val_clean = val.replace(',', '').replace('+', '').replace(' ', '')
                is_negative = False
                if any(x in val_clean for x in ['▲', '▼', '－', '−', '‐', '-']): is_negative = True
                num_only = re.sub(r'[^\d.]', '', val_clean)
                if not num_only: return 0
                try:
                    number = int(float(num_only))
                    return -number if is_negative else number
                except: return 0
            df[col] = s_raw.apply(safe_convert)
    
    week_chars = ['月', '火', '水', '木', '金', '土', '日']
    df['曜日'] = df['日付'].dt.dayofweek.apply(lambda x: week_chars[x])
    df['週'] = (df['日付'].dt.day - 1) // 7 + 1
    df['REG確率'] = df.apply(lambda x: x['G数']/x['RB'] if x['RB'] > 0 else 9999, axis=1)
    df['メーカー'] = df['機種'].apply(detect_maker)
    df['末尾'] = df['台番'].astype(str).str[-1]
    df = df.sort_values(['台番', '日付'])
    df['前日差枚'] = df.groupby('台番', observed=False)['差枚'].shift(1)
    df['前日G数'] = df.groupby('台番', observed=False)['G数'].shift(1)
    df['Δ差枚'] = df['差枚'] - df['前日差枚']
    return df

# ==========================================
# サイドバー
# ==========================================
st.sidebar.title("🎰 スロット攻略 Pro")
store_names = list(logic.STORE_CONFIG.keys())
selected_store = st.sidebar.selectbox("🏟️ 店舗を選択", store_names)
store_info = logic.STORE_CONFIG[selected_store]
st.sidebar.info(f"📅 {store_info.get('event_text', '情報なし')}")

current_dir = os.path.dirname(os.path.abspath(__file__))
# 公開用データフォルダ (public_data/店舗名) を想定
data_folder = os.path.join(current_dir, "public_data", selected_store)
# もし上記になければ、ルート直下の店舗名フォルダを探す
if not os.path.exists(data_folder):
    data_folder = os.path.join(current_dir, selected_store)

df_all_raw = load_and_process_data(data_folder)

st.sidebar.divider()
st.sidebar.subheader("🔍 分析条件設定")

df_filtered = pd.DataFrame()

if not df_all_raw.empty:
    max_date = df_all_raw['日付'].max().date()
    min_date = df_all_raw['日付'].min().date()
    period_option = st.sidebar.selectbox("対象期間", ["全期間", "直近1週間", "直近2週間", "直近1ヶ月", "直近3ヶ月", "カスタム指定"], index=0, label_visibility="collapsed")
    start_dt, end_dt = min_date, max_date
    if period_option == "カスタム指定":
        custom_range = st.sidebar.date_input("日付範囲", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        if isinstance(custom_range, tuple) and len(custom_range) == 2: start_dt, end_dt = custom_range
    elif period_option != "全期間":
        days_map = {"直近1週間":7, "直近2週間":14, "直近1ヶ月":30, "直近3ヶ月":90}
        days_back = days_map.get(period_option, 30)
        start_dt = max_date - timedelta(days=days_back - 1)
        end_dt = max_date
    mask = (df_all_raw['日付'].dt.date >= start_dt) & (df_all_raw['日付'].dt.date <= end_dt)
    df_period = df_all_raw.loc[mask].copy()

    with st.sidebar.expander("⚡ 特定日・曜日の絞り込み", expanded=False):
        custom_days_str = st.text_input("特定日", placeholder="例: 9, 19, 29")
        selected_weekdays = st.multiselect("曜日", ["月", "火", "水", "木", "金", "土", "日"], default=[])
        selected_weeks = st.multiselect("週 (第n週)", [1, 2, 3, 4, 5], default=[])
        is_doublet = st.checkbox("月日ゾロ目 (1/1, 2/2...)")

    df_filtered = df_period.copy()
    filter_info = []
    if custom_days_str:
        try:
            target_days = [int(d) for d in custom_days_str.replace("、", ",").replace(" ", "").split(",") if d.isdigit()]
            if target_days: df_filtered = df_filtered[df_filtered['日付'].dt.day.isin(target_days)]; filter_info.append(f"日付: {target_days}")
        except: pass
    if selected_weekdays: df_filtered = df_filtered[df_filtered['曜日'].isin(selected_weekdays)]; filter_info.append(f"曜日: {selected_weekdays}")
    if selected_weeks: df_filtered = df_filtered[df_filtered['週'].isin(selected_weeks)]; filter_info.append(f"週: 第{selected_weeks}")
    if is_doublet:
        df_filtered = df_filtered[df_filtered['日付'].dt.month == df_filtered['日付'].dt.day]
        filter_info.append("月日ゾロ目")

    df_all = df_filtered.copy()
else:
    df_all = pd.DataFrame()

# ----------------------------------------------
# 🛠 データの更新・収集 (公開用では機能しないがUIとして残す)
# ----------------------------------------------
with st.sidebar.expander("🛠 データの更新・収集", expanded=False):
    now = datetime.now()
    is_safe_time = (now.hour == 8) or (now.hour == 9)
    st.write(f"**{selected_store}** のデータを取得します。")
    if is_safe_time: st.success("✅ 現在はデータ収集可能です (8:00〜9:59)")
    else: st.error("⛔ 時間外のため機能ロック中 (8:00〜9:59 のみ可能)")
    
    today = datetime.now().date()
    date_range_scrape = st.date_input("取得範囲", value=(today - timedelta(days=7), today - timedelta(days=1)), max_value=today, key="scrape_date")
    max_workers = st.slider("並列スレッド数", 1, 5, 2)
    
    col_b1, col_b2 = st.columns(2)
    if st.button(f"この店舗のみ", type="secondary", disabled=not is_safe_time): 
        if isinstance(date_range_scrape, tuple) and len(date_range_scrape) == 2:
            st.warning("⚠️ 公開版ではスクレイピング機能は制限されています。管理者にご連絡ください。")

    if st.button("🔄 全店舗まとめて収集", type="primary", disabled=not is_safe_time):
        st.warning("⚠️ 公開版ではスクレイピング機能は制限されています。管理者にご連絡ください。")

# ---------------------------------------------------------
# ▼ AI分析用データ出力 (修正版)
# ---------------------------------------------------------
st.sidebar.divider()
st.sidebar.subheader("🤖 AI分析用データ出力")

if 'df_filtered' in locals() and not df_filtered.empty:
    ai_export_df = df_filtered.copy()
    if '差枚' in ai_export_df.columns:
        ai_export_df['結果'] = ai_export_df['差枚'].apply(lambda x: 'Win' if x > 0 else 'Lose')
    csv_data = ai_export_df.to_csv(index=False).encode('utf-8-sig')
    f_name = f"{selected_store}_AI分析用.csv"
    st.sidebar.download_button(
        label="📥 AI分析用CSVをダウンロード",
        data=csv_data,
        file_name=f_name,
        mime="text/csv",
        help="このファイルをChatGPTやClaudeにアップロードして、傾向を聞いてみてください。"
    )
else:
    st.sidebar.warning("データが表示されていません")

st.sidebar.divider()

# ==========================================
# メイン画面
# ==========================================
st.title(f"📊 {selected_store} 攻略分析")

if df_all.empty:
    st.warning("条件に合うデータがありません。サイドバーでデータを収集するか、期間を変更してください。")
    st.stop()

if filter_info: st.info(f"⚡ フィルター: {' / '.join(filter_info)}")

tab1, tab2, tab3, tab4 = st.tabs(["📅 日別レポート", "🔥 店長推し分析 (機種)", "🕵️‍♀️ 不発・並び発掘", "🔍 鉄板台サーチ"])

# --- Tab 1: 日別レポート ---
with tab1:
    st.subheader("📅 日別サマリー (3ヶ月一覧)")
    sorted_dates = sorted(df_all['日付'].unique(), reverse=True)
    memos = load_memos()

    with st.expander("📝 メモを編集する", expanded=False):
        if len(sorted_dates) > 0:
            target_date = st.selectbox("日付を選択", sorted_dates, key="memo_date_selector")
            date_key_edit = target_date.strftime('%Y-%m-%d')
            memo_key = f"{selected_store}_{date_key_edit}"
            current_memo = memos.get(memo_key, "")
            c_memo_in, c_memo_btn = st.columns([4, 1])
            with c_memo_in: new_memo_val = st.text_input("メモ内容", value=current_memo, placeholder="例: イベント日、全台系あり", label_visibility="collapsed")
            with c_memo_btn:
                if st.button("保存", type="primary", key="save_memo_btn"):
                    save_memo(date_key_edit, new_memo_val, selected_store)
                    st.toast(f"{date_key_edit} のメモを保存しました")
                    time.sleep(1); st.rerun()
        else: st.info("データがありません")

    with st.expander("📂 その日の全台データを見る (機種・末尾分析)", expanded=False):
        if len(sorted_dates) > 0:
            c_date, c_model = st.columns([1, 2])
            with c_date: view_date = st.selectbox("日付", sorted_dates, key="raw_data_date_selector")
            raw_df_day = df_all[df_all['日付'] == view_date].copy()
            
            st.markdown("##### 🔢 末尾別 平均差枚数")
            end_stats_graph = raw_df_day.groupby('末尾').agg(平均差枚=('差枚', 'mean')).reset_index()
            fig_end = px.bar(end_stats_graph, x='末尾', y='平均差枚', color='平均差枚', color_continuous_scale='Bluered_r')
            st.plotly_chart(fig_end) # 引数なしでWarning回避
            
            def calc_prob_safe(g, c): return round(g / c, 1) if c > 0 else 9999.0
            raw_df_day['BIG確率'] = raw_df_day.apply(lambda x: calc_prob_safe(x['G数'], x['BB']), axis=1)
            raw_df_day['合算確率'] = raw_df_day.apply(lambda x: calc_prob_safe(x['G数'], x['BB'] + x['RB']), axis=1)
            
            all_models = sorted(raw_df_day['機種'].unique())
            with c_model: selected_models = st.multiselect("機種で絞り込み", all_models, placeholder="機種を選択 (未選択で全表示)")
            if selected_models: raw_df_day = raw_df_day[raw_df_day['機種'].isin(selected_models)]
            
            final_df = raw_df_day[['機種', '台番', '末尾', '差枚', 'G数', 'BB', 'RB', '合成', 'BIG確率', 'REG確率', '合算確率']].sort_values('差枚', ascending=False)
            # Warning回避: width="stretch"
            st.dataframe(final_df.style.format({'G数': '{:,}', 'BIG確率': '1/{:.1f}', 'REG確率': '1/{:.1f}', '合算確率': '1/{:.1f}'}), column_config={"差枚": st.column_config.NumberColumn("差枚", format="%+d"), "機種": st.column_config.TextColumn("機種名", width="medium")}, height=400, width="stretch")
            total_diff = int(final_df['差枚'].sum()); st.caption(f"📊 表示中の合計: {len(final_df)}台 / 総差枚: {total_diff:+d}枚")
        else: st.info("データがありません")

    with st.expander("ℹ️ アイコンの意味・判定ルール (クリックで開閉)", expanded=True):
        st.markdown("""
        #### 🔥 強末尾
        以下の3つの条件をすべて満たす優秀な末尾です。
        * **勝率**: 50%以上
        * **勝利台平均G数**: 4,000回転以上
        * **全体平均差枚**: プラス
        
        ---
        
        #### その他のアイコン
        * **★ 全勝/鉄板**: 勝率 100% かつ 平均G数 7,000G以上
        * **◎ 絶好調**: 勝率 66%以上 かつ 差枚+1,500枚 かつ 平均G数 7,000G以上
        * **🌀 ぶん回し**: 機種平均 7,000G以上
        * **○ 好調**: 勝率 50%以上 かつ 勝ち台平均 7,000G以上
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    ITEMS_PER_PAGE = 90
    total_pages = max(1, -(-len(sorted_dates) // ITEMS_PER_PAGE)) 
    if total_pages > 1:
        st.markdown('<div class="pagination-box">', unsafe_allow_html=True)
        col_p1, col_p2, col_p3 = st.columns([1, 2, 1])
        with col_p2: current_page = st.number_input("ページ切り替え", 1, total_pages, 1, key="tab1_main_pagination_v2")
        st.markdown('</div>', unsafe_allow_html=True)
    else: current_page = 1
    
    display_dates = sorted_dates[(current_page - 1) * ITEMS_PER_PAGE : current_page * ITEMS_PER_PAGE]
    table_headers = '<thead><tr><th class="td-date">日付</th><th class="td-total">総差枚</th><th class="td-avg">平均</th><th class="td-g">平均G</th><th class="td-end">強末尾</th><th class="td-models">主力機種・メモ</th></tr></thead>'
    table_rows = ''

    for date_val in display_dates:
        df_day = df_all[df_all['日付'] == date_val].copy()
        date_key = date_val.strftime('%Y-%m-%d')
        day_week = df_day['曜日'].iloc[0]
        total_diff = int(df_day['差枚'].sum())
        avg_diff = int(df_day['差枚'].mean())
        avg_g = int(df_day['G数'].mean())
        is_event = str(date_val.day) in store_info.get('event_text', '')
        date_str = f"{date_val.strftime('%m/%d')}({day_week})"
        if is_event: date_str = f"🔥 {date_str}"
        total_cls = "val-plus" if total_diff > 0 else "val-minus"
        avg_cls = "val-plus" if avg_diff > 0 else "val-minus"
        
        # --- 末尾集計（台数・勝利数追加） ---
        end_stats_all = df_day.groupby('末尾', observed=False).agg(
            平均差枚=('差枚', 'mean'),
            勝率=('差枚', lambda x: (x > 0).mean()),
            全台数=('台番', 'count'),
            勝利台数=('差枚', lambda x: (x > 0).sum())
        ).reset_index()
        
        df_day_win = df_day[df_day['差枚'] > 0]
        if not df_day_win.empty:
            end_stats_win = df_day_win.groupby('末尾', observed=False).agg(
                勝利台平均G数=('G数', 'mean'),
                勝利台平均差枚=('差枚', 'mean')
            ).reset_index()
        else:
            end_stats_win = pd.DataFrame(columns=['末尾', '勝利台平均G数', '勝利台平均差枚'])
        
        end_stats = pd.merge(end_stats_all, end_stats_win, on='末尾', how='left').fillna(0)
        strong_ends = end_stats[(end_stats['勝率'] >= 0.5) & (end_stats['勝利台平均G数'] >= 4000) & (end_stats['平均差枚'] > 0)].sort_values('平均差枚', ascending=False)

        if not strong_ends.empty:
            best_end = strong_ends.iloc[0]
            win_count = int(best_end['勝利台数'])
            total_count = int(best_end['全台数'])
            end_html = f"🔢{best_end['末尾']} ({win_count}/{total_count})<br><span style='font-size:0.8rem; color:#d63384;'>全{int(best_end['平均差枚']):+}/勝{int(best_end['勝利台平均差枚']):+}</span>"
        else: end_html = "-"

        win_machines = df_day[df_day['差枚'] > 0]
        win_g_means = win_machines.groupby('機種')['G数'].mean() if not win_machines.empty else pd.Series(dtype=float)
        
        # --- 機種別集計（勝利台数追加） ---
        model_stats = df_day.groupby('機種', observed=False).agg(
            平均差枚=('差枚', 'mean'), 
            勝率=('差枚', lambda x: (x > 0).mean()),
            勝利台数=('差枚', lambda x: (x > 0).sum()),
            平均G数=('G数', 'mean'), 
            台数=('台番', 'count')
        ).reset_index()
        
        models_html_parts = []
        displayed_models = set()
        memo_key = f"{selected_store}_{date_key}"
        memo = memos.get(memo_key, "")
        if memo: models_html_parts.append(f'<span class="memo-item">📝 {memo}</span>')

        candidates = model_stats[model_stats['台数'] >= 3].sort_values('平均差枚', ascending=False)
        for _, row in candidates.iterrows():
            icon = ""
            m_name = row['機種']
            win_avg_g = win_g_means.get(m_name, 0)
            if row['勝率'] == 1.0 and row['平均G数'] >= 7000: icon = "<span class='icon-star'>★</span>"
            elif row['勝率'] >= 0.66 and row['平均差枚'] >= 1500 and row['平均G数'] >= 7000: icon = "<span class='icon-double'>◎</span>"
            elif row['平均G数'] >= 7000: icon = "<span class='icon-spin'>🌀</span>"
            if not icon and row['勝率'] >= 0.5 and win_avg_g >= 7000: icon = "<span class='icon-circle'>○</span>"
            if icon and m_name not in displayed_models:
                w_num = int(row['勝利台数'])
                t_num = int(row['台数'])
                models_html_parts.append(f"<span class='model-line'>{icon} {m_name}({w_num}/{t_num} {int(row['平均差枚']):+})</span>"); displayed_models.add(m_name)

        models_html = "".join(models_html_parts) if models_html_parts else "-"
        table_rows += f'<tr><td class="td-date">{date_str}</td><td class="td-total {total_cls}">{total_diff:+,}</td><td class="td-avg {avg_cls}">{avg_diff:+,}</td><td class="td-g">{avg_g:,}</td><td class="td-end">{end_html}</td><td class="td-models">{models_html}</td></tr>'

    if len(display_dates) > 0: st.markdown(f'<table class="custom-table">{table_headers}<tbody>{table_rows}</tbody></table>', unsafe_allow_html=True)
    else: st.info("表示できるデータがありません")

# --- Tab 2: 🔥 店長推し分析 (機種別) + 詳細履歴 (コンパクト化移植) ---
with tab2:
    st.subheader("🔥 店長推し分析 (機種別・全台データ)")
    if not df_all.empty:
        stats = df_all.groupby('機種', observed=False).agg(
            平均差枚=('差枚', 'mean'), 
            勝率=('差枚', lambda x: (x>0).mean()*100), 
            平均G数=('G数', 'mean'), 
            サンプル数=('台番', 'count'), 
            合計差枚=('差枚', 'sum')
        ).reset_index()
        valid = stats[stats['サンプル数'] >= 5].copy()
        if not valid.empty:
            with st.expander("📊 機種全体の相関図を開く", expanded=False):
                c_view1, c_view2 = st.columns(2)
                show_labels = c_view1.toggle("機種名を表示", value=True)
                show_only_plus = c_view2.toggle("プラス機種のみ", value=False)
                if show_only_plus: valid = valid[valid['平均差枚'] > 0]
                fig = px.scatter(valid, x="勝率", y="平均差枚", size="平均G数", color="合計差枚", hover_name="機種", text="機種" if show_labels else None, color_continuous_scale=['blue', 'white', 'red'], range_color=[-30000, 30000], size_max=60)
                fig.update_layout(height=500, xaxis_title="勝率 (%)", yaxis_title="平均差枚 (枚)")
                if show_labels: fig.update_traces(textposition='top center')
                st.plotly_chart(fig)

    st.markdown("---")
    st.subheader("🕵️‍♂️ 台番別・詳細履歴 (設定判別特化)")
    st.caption("ジャグラー系は **ブドウ逆算** と **REG確率からの設定推測** を自動表示します。")

    if not df_all_raw.empty:
        model_list = sorted(df_all_raw['機種'].unique())
        default_idx = 0
        for i, m in enumerate(model_list):
            if "マイジャグ" in m: default_idx = i; break
        
        target_model = st.selectbox("機種を選択", model_list, index=default_idx, key="detail_model_select_html")
        is_juggler = any(kw in target_model for kw in ["ジャグラー", "マイジャグ", "ファンキー", "アイム", "ゴージャグ", "ハッピー", "ガールズ", "ミスター", "ミラクル"])

        subset = df_all_raw[df_all_raw['機種'] == target_model].copy()
        
        if not subset.empty:
            latest_date = subset['日付'].max()
            start_date = latest_date - timedelta(days=6)
            df_view = subset[subset['日付'] >= start_date].copy()
            dates = sorted(df_view['日付'].unique(), reverse=True)
            machines = sorted(df_view['台番'].unique())
            
            df_view['3日フラグ'] = df_view['日付'] >= (latest_date - timedelta(days=2))
            
            machine_stats = {}
            for m in machines:
                m_rows = df_view[df_view['台番'] == m]
                sum_7 = m_rows['差枚'].sum()
                sum_3 = m_rows[m_rows['3日フラグ']]['差枚'].sum()
                machine_stats[m] = {'sum3': sum_3, 'sum7': sum_7}
            
            data_map = {}
            for idx, row in df_view.iterrows():
                m = row['台番']
                d = row['日付'].strftime('%Y-%m-%d')
                data_map.setdefault(m, {})[d] = row

            # ▼▼▼ コンパクト化HTML実装 (移植) ▼▼▼
            html = """<style>
.history-table { width: 100%; border-collapse: collapse; font-family: "Meiryo", sans-serif; font-size: 0.75rem; } 
.history-table th { background-color: #f0f2f6; border: 1px solid #ccc; padding: 4px 2px; text-align: center; white-space: nowrap; font-size: 0.75rem; position: sticky; top: 0; z-index: 10; height: 30px; }
.history-table td { border: 1px solid #ccc; padding: 2px; text-align: center; vertical-align: middle; background-color: #fff; min-width: 95px; height: 1px; } 
.h-machine { font-weight: bold; font-size: 0.9rem; background-color: #fafafa; position: sticky; left: 0; z-index: 9; border-right: 2px solid #bbb !important; width: 50px; }
.h-total { font-weight: bold; font-size: 0.85rem; }
.cell-container { display: flex; flex-direction: column; justify-content: center; height: 100%; min-height: 55px; } 
.row-top { display: flex; justify-content: space-between; align-items: baseline; border-bottom: 1px solid #eee; margin-bottom: 1px; padding-bottom: 1px; }
.cell-diff { font-size: 0.95rem; font-weight: bold; line-height: 1; }
.cell-g { font-size: 0.7rem; color: #666; }
.row-mid { font-size: 0.7rem; color: #444; line-height: 1.1; text-align: center; white-space: nowrap; }
.prob-box { background-color: #f4f4f4; padding: 0 2px; border-radius: 2px; margin-right: 2px; font-weight: bold; }
.row-bot { font-size: 0.7rem; color: purple; font-weight: bold; margin-top: 1px; line-height: 1; border-top: 1px dotted #eee; }
.est-tag { font-size: 0.65rem; display: inline-block; padding: 0px 3px; border-radius: 3px; color: white; margin-left: 2px; vertical-align: middle; }
.est-6 { background-color: #e91e63; }
.est-456 { background-color: #ff9800; }
.est-low { background-color: #fdd835; color: #333; }
.c-plus { color: #d32f2f; }
.c-minus { color: #1e88e5; }
</style>
<div style="overflow-x: auto; max-height: 800px; overflow-y: auto; border: 1px solid #ccc;">
<table class="history-table">
<thead>
<tr>
<th class="h-machine" style="z-index: 11;">台番</th>
<th>3日計</th>
<th>7日計</th>"""
            for d in dates: html += f"<th>{d.strftime('%m/%d')}</th>"
            html += "</tr></thead><tbody>"
            
            for m in machines:
                stats = machine_stats[m]
                cls_3 = "c-plus" if stats['sum3'] > 0 else "c-minus"
                cls_7 = "c-plus" if stats['sum7'] > 0 else "c-minus"
                html += f"<tr><td class='h-machine'>{m}</td><td class='h-total {cls_3}'>{stats['sum3']:+d}</td><td class='h-total {cls_7}'>{stats['sum7']:+d}</td>"
                
                for d in dates:
                    d_key = d.strftime('%Y-%m-%d')
                    if d_key in data_map.get(m, {}):
                        row = data_map[m][d_key]
                        diff = int(row['差枚']); g = int(row['G数']); bb = int(row['BB']); rb = int(row['RB'])
                        total_bon = bb + rb
                        t_prob = f"1/{g//total_bon}" if total_bon > 0 else "-"
                        # 確率分母のみ表示してスペース節約
                        bb_denom = f"/{g//bb}" if bb > 0 else "-"
                        rb_denom = f"/{g//rb}" if rb > 0 else "-"
                        diff_cls = "c-plus" if diff > 0 else "c-minus"
                        
                        top_html = f"<div class='row-top'><span class='cell-diff {diff_cls}'>{diff:+d}</span><span class='cell-g'>{g}G</span></div>"
                        mid_html = f"<div class='row-mid'><span class='prob-box'>合{t_prob}</span> B{bb} R{rb}</div>"
                        
                        jug_html = ""
                        if is_juggler and g > 500:
                            grape = calc_grape_prob_v2(row)
                            if 3.5 <= grape <= 9.0:
                                est = ""
                                reg_denom = g / rb if rb > 0 else 9999
                                if reg_denom <= 255 and grape <= 5.8: est = "<span class='est-tag est-6'>6?</span>"
                                elif reg_denom <= 280 and grape <= 6.0: est = "<span class='est-tag est-456'>45?</span>"
                                elif reg_denom <= 320 and grape <= 6.2: est = "<span class='est-tag est-low'>34?</span>"
                                jug_html = f"<div class='row-bot'>🍇1/{grape:.1f}{est}</div>"
                        
                        html += f"<td><div class='cell-container'>{top_html}{mid_html}{jug_html}</div></td>"
                    else: html += "<td style='background:#f9f9f9; color:#ccc'>-</td>"
                html += "</tr>"
            html += "</tbody></table></div>"
            st.markdown(html, unsafe_allow_html=True)
        else: st.warning("データが見つかりません")

with tab3:
    st.subheader("🕵️‍♀️ 不発・塊検知")
    unlucky = df_all[(df_all['G数']>=5000) & (df_all['差枚']<=-500) & (df_all['REG確率']<=350)]
    if not unlucky.empty: st.error("不発ジャグラー候補"); st.dataframe(unlucky[['日付', '機種', '台番', '差枚', 'G数', 'RB', 'REG確率']], width="stretch")
    dates = df_all['日付'].dt.date.unique()
    if len(dates) > 0:
        d = st.selectbox("並び検知日", dates)
        day_df = df_all[df_all['日付'].dt.date == d].sort_values('台番')
        day_df['MA3_G'] = day_df['G数'].rolling(3, center=True).mean()
        day_df['MA3_Diff'] = day_df['差枚'].rolling(3, center=True).mean()
        found = day_df[(day_df['MA3_G']>=7000) & (day_df['MA3_Diff']>=1500)]
        if not found.empty:
            st.success("🔥 並び候補発見")
            for i, r in found.iterrows(): st.table(day_df[(day_df['台番'] >= r['台番']-1) & (day_df['台番'] <= r['台番']+1)][['機種', '台番', '差枚', 'G数']])

with tab4:
    st.header("🔍 鉄板台サーチ & 🍇推定ブドウ逆算")
    target_src = st.radio("データソース", ["現在選択中の期間 (サイドバー)", "全期間 (読込済データ)"], horizontal=True)
    base_df = df_all_raw.copy() if "全期間" in target_src else df_all.copy()
    
    def calc_p(g, c): return round(g/c, 1) if c>0 else 9999.0
    base_df['BIG確率'] = base_df.apply(lambda x: calc_p(x['G数'], x['BB']), axis=1)
    base_df['REG確率'] = base_df.apply(lambda x: calc_p(x['G数'], x['RB']), axis=1)
    base_df['合算確率'] = base_df.apply(lambda x: calc_p(x['G数'], x['BB']+x['RB']), axis=1)
    
    base_df['🍇推定ブドウ'] = base_df.apply(calc_grape_prob, axis=1)
    base_df['🍇確率'] = base_df['🍇推定ブドウ'].apply(lambda x: f"1/{x:.1f}" if x > 0 else "-")

    c1, c2 = st.columns(2)
    min_g = int(c1.selectbox("回転数以上", [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000], index=2))
    min_d = c2.number_input("差枚数以上", value=1000, step=100)
    
    st.caption("確率フィルター (任意)")
    cp1, cp2, cp3 = st.columns(3)
    use_b = cp1.checkbox("BIG"); bv = cp1.number_input("1/", value=250.0, disabled=not use_b)
    use_r = cp2.checkbox("REG"); rv = cp2.number_input("1/", value=300.0, disabled=not use_r)
    use_t = cp3.checkbox("合算"); tv = cp3.number_input("1/", value=130.0, disabled=not use_t)

    res = base_df[(base_df['G数']>=min_g) & (base_df['差枚']>=min_d)].copy()
    if use_b: res = res[res['BIG確率']<=bv]
    if use_r: res = res[res['REG確率']<=rv]
    if use_t: res = res[res['合算確率']<=tv]

    if not res.empty:
        st.markdown('<div class="analysis-box">', unsafe_allow_html=True)
        st.subheader("📈 傾向分析 (エース台番)")
        stats = res.groupby(['台番', '機種']).agg(回数=('日付', 'count'), 平均差枚=('差枚', 'mean'), 直近=('日付', 'max')).reset_index().sort_values(['回数', '直近'], ascending=[False, False])
        
        if not stats.empty:
            stats['Label'] = stats['台番'].astype(str) + " (" + stats['機種'] + ")"
            st.markdown("##### 🥇 条件達成回数ランキング")
            fig = px.bar(stats.head(15), x='回数', y='Label', orientation='h', color='平均差枚', text=stats.head(15)['直近'].dt.strftime('%m/%d'))
            fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
            st.plotly_chart(fig, width="stretch")
            st.markdown("##### 🥧 機種別シェア")
            pie = px.pie(res['機種'].value_counts().reset_index(), values='count', names='機種', hole=0.4)
            st.plotly_chart(pie, width="stretch")
            
        st.markdown("##### 📋 エース台番リスト")
        # Warning回避: width="stretch"
        st.dataframe(stats.head(20).style.format({'平均差枚':'{:.0f}'}), width="stretch")
        st.markdown('</div>', unsafe_allow_html=True)
        st.subheader("📝 抽出データ全リスト (ブドウ逆算付き)")
        
        display_cols = ['日付','機種','台番','🍇確率','差枚','G数','合算確率','BIG確率','REG確率']
        
        # Warning回避: width="stretch"
        st.dataframe(
            res[display_cols].sort_values('差枚', ascending=False)
            .style.format({
                '日付': '{:%Y-%m-%d}',
                '差枚': '{:+d}',
                'BIG確率': '1/{:.1f}',
                'REG確率': '1/{:.1f}',
                '合算確率': '1/{:.1f}'
            })
            .map(lambda x: 'background-color: #ffcccc' if '1/5.' in str(x) else '', subset=['🍇確率']),
            width="stretch"
        )
    else:
        st.warning("条件に合う台はありません")