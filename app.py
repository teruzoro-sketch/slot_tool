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
# IP取得用
from streamlit.web.server.websocket_headers import _get_websocket_headers 

import logic

# ==========================================
# 設定・定数エリア
# ==========================================
st.set_page_config(page_title="Slot Master Pro", layout="wide", page_icon="🎰")

MEMO_FILE = "daily_memos.json"

st.markdown("""
    <style>
        .main .block-container {
            max-width: 100% !important;
            padding: 1rem 1rem 3rem 1rem !important;
        }
        div[data-testid="stDataFrame"] div[role="gridcell"] {
            white-space: pre-wrap !important;
            line-height: 1.5 !important;
            display: flex;
            align-items: center;
        }
        .metric-card {
            background-color: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px;
            padding: 15px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.05); margin-bottom: 10px;
        }
        .metric-label { font-size: 0.85rem; color: #6c757d; margin-bottom: 5px; }
        .metric-value { font-size: 1.4rem; font-weight: 700; color: #343a40; }
        .val-pos { color: #dc3545 !important; } .val-neg { color: #28a745 !important; } 
        .minrepo-row {
            display: flex; align-items: center; justify-content: space-between;
            padding: 10px 15px; margin-bottom: 8px; background-color: #fff;
            border: 1px solid #ddd; border-left: 5px solid #ccc; border-radius: 4px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05); transition: all 0.2s;
        }
        .minrepo-row:hover { box-shadow: 0 4px 8px rgba(0,0,0,0.1); transform: translateY(-1px); }
        .mr-date { flex: 2; font-weight: bold; font-size: 1.1em; }
        .mr-total { flex: 1.5; text-align: right; font-weight: bold; color: #555; }
        .mr-avg { flex: 1.5; text-align: right; font-weight: bold; }
        .mr-g { flex: 1.5; text-align: right; font-size: 0.95em; color: #666; }
        .mr-win { flex: 1.5; text-align: right; font-size: 0.95em; color: #666; }
        .mr-memo { flex: 0.5; text-align: center; font-size: 1.2em; }
        .border-pos { border-left-color: #dc3545 !important; background-color: #fff5f5 !important; }
        .border-neg { border-left-color: #6c757d !important; }
        .recommend-box { border: 2px solid #ffc107; background-color: #fffbf2; padding: 15px; border-radius: 10px; margin-bottom: 20px; }
        .analysis-box { background-color: #e3f2fd; border: 1px solid #90caf9; border-radius: 8px; padding: 15px; margin-top: 20px; }
        .pagination-box { text-align: center; padding: 10px; background: #f0f2f6; border-radius: 10px; margin-bottom: 20px; }
        .custom-table { width: 100%; border-collapse: collapse; font-size: 14px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }
        .custom-table th { background-color: #f8f9fa; padding: 12px 8px; text-align: center; border: 1px solid #dee2e6; font-weight: bold; color: #495057; }
        .custom-table td { padding: 12px 10px; border: 1px solid #dee2e6; vertical-align: top; background-color: #fff; line-height: 1.6; color: #333; }
        .td-date   { width: 12%; text-align: center; font-weight: bold; white-space: nowrap; color: #333; }
        .td-total  { width: 10%; text-align: right; font-weight: bold; font-size: 15px; color: #333; }
        .td-avg    { width: 8%; text-align: right; font-weight: bold; color: #333; }
        .td-g      { width: 10%; text-align: right; color: #666; font-size: 13px; }
        .td-models { width: 60%; text-align: left; font-size: 13px; color: #333; }
        .val-plus { color: #d32f2f !important; }
        .val-minus { color: #333 !important; }
        .model-line { display: inline-block; margin-right: 12px; margin-bottom: 4px; }
        .memo-item { display: block; color: #0d6efd; font-weight: bold; margin-bottom: 6px; background-color: #e7f1ff; padding: 4px 8px; border-radius: 4px; }
        .icon-star { color: #ff9800; font-weight: bold; font-size: 1.1em; } 
        .icon-double { color: #e91e63; font-weight: bold; font-size: 1.1em; } 
        .icon-circle { color: #4caf50; font-weight: bold; } 
        .icon-spin { color: #6610f2; font-weight: bold; font-size: 1.1em; }
    </style>
""", unsafe_allow_html=True)

UNLUCKY_ATYPE_COND = {"min_games": 5000, "max_diff": -500, "min_reg_prob": 350}
UNLUCKY_AT_COND = {"min_games": 7500, "max_diff": -1000}

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
# ▼ 【追加】ここにイベント情報を表示するようにしました
st.sidebar.info(f"📅 {store_info.get('event_text', '情報なし')}")

current_dir = os.path.dirname(os.path.abspath(__file__))
data_folder = os.path.join(current_dir, selected_store)
df_all_raw = load_and_process_data(data_folder)

st.sidebar.divider()
st.sidebar.subheader("🔍 分析条件設定")

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
        # ▼ 【追加】月日ゾロ目フィルター
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
    
    # ▼ 【追加】月日ゾロ目フィルターロジック
    if is_doublet:
        df_filtered = df_filtered[df_filtered['日付'].dt.month == df_filtered['日付'].dt.day]
        filter_info.append("月日ゾロ目")

    df_all = df_filtered.copy()
else:
    df_all = pd.DataFrame()

# ----------------------------------------------
# 🛠 データの更新・収集
# ----------------------------------------------
with st.sidebar.expander("🛠 データの更新・収集", expanded=False):
    now = datetime.now()
    is_safe_time = (now.hour == 8) or (now.hour == 9)
    # is_safe_time = True 
    
    st.write(f"**{selected_store}** のデータを取得します。")
    
    if is_safe_time:
        st.success("✅ 現在はデータ収集可能です (8:00〜9:59)")
    else:
        st.error("⛔ 時間外のため機能ロック中 (8:00〜9:59 のみ可能)")
    
    today = datetime.now().date()
    date_range_scrape = st.date_input("取得範囲", value=(today - timedelta(days=7), today - timedelta(days=1)), max_value=today, key="scrape_date")
    max_workers = st.slider("並列スレッド数", 1, 5, 2)
    
    col_b1, col_b2 = st.columns(2)
    
    if st.button(f"この店舗のみ", type="secondary", disabled=not is_safe_time): 
        if isinstance(date_range_scrape, tuple) and len(date_range_scrape) == 2:
            with st.spinner(f"{selected_store} を収集中..."):
                logic.run_scraping(selected_store, date_range_scrape[0], date_range_scrape[1], max_workers)
                st.cache_data.clear()
                st.rerun()

    if st.button("🔄 全店舗まとめて収集", type="primary", disabled=not is_safe_time):
        if isinstance(date_range_scrape, tuple) and len(date_range_scrape) == 2:
            s_date, e_date = date_range_scrape
            total_stores = len(store_names)
            
            progress_bar_all = st.progress(0)
            status_text_all = st.empty()
            
            for i, target_store in enumerate(store_names):
                if not logic.is_safe_scrape_time():
                    st.error("⏰ 時間オーバーのため中断しました")
                    break

                status_text_all.write(f"⏳ [{i+1}/{total_stores}] **{target_store}** のデータを収集中...")
                try:
                    logic.run_scraping(target_store, s_date, e_date, max_workers)
                    st.toast(f"✅ {target_store} 完了")
                except Exception as e:
                    st.error(f"❌ {target_store} エラー: {e}")
                
                progress_bar_all.progress((i + 1) / total_stores)
                time.sleep(1.5)
            
            status_text_all.success("🎉 全店舗の収集が完了しました！")
            time.sleep(2)
            st.cache_data.clear()
            st.rerun()

st.sidebar.divider()

# ==========================================
# メイン画面
# ==========================================
st.title(f"📊 {selected_store} 攻略分析")

if df_all.empty:
    st.warning("条件に合うデータがありません。サイドバーでデータを収集するか、期間を変更してください。")
    st.stop()

if filter_info: st.info(f"⚡ フィルター: {' / '.join(filter_info)}")

tab1, tab2, tab3, tab4 = st.tabs(["📅 日別レポート", "🔥 店長推し分析 (機種)", "🕵️‍♀️ 不発・並び発掘", "🔍 鉄板台サーチ＆多角分析"])

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
            with c_memo_in:
                new_memo_val = st.text_input("メモ内容", value=current_memo, placeholder="例: イベント日、全台系あり", label_visibility="collapsed")
            with c_memo_btn:
                if st.button("保存", type="primary", key="save_memo_btn"):
                    save_memo(date_key_edit, new_memo_val, selected_store)
                    st.toast(f"{date_key_edit} のメモを保存しました")
                    time.sleep(1)
                    st.rerun()
        else:
            st.info("データがありません")

    with st.expander("📂 その日の全台データを見る (機種絞り込み)", expanded=False):
        if len(sorted_dates) > 0:
            c_date, c_model = st.columns([1, 2])
            with c_date:
                view_date = st.selectbox("日付", sorted_dates, key="raw_data_date_selector")
            
            raw_df_day = df_all[df_all['日付'] == view_date].copy()
            def calc_prob_safe(g, c): return round(g / c, 1) if c > 0 else 9999.0
            raw_df_day['BIG確率'] = raw_df_day.apply(lambda x: calc_prob_safe(x['G数'], x['BB']), axis=1)
            raw_df_day['合算確率'] = raw_df_day.apply(lambda x: calc_prob_safe(x['G数'], x['BB'] + x['RB']), axis=1)
            
            all_models = sorted(raw_df_day['機種'].unique())
            with c_model:
                selected_models = st.multiselect("機種で絞り込み", all_models, placeholder="機種を選択 (未選択で全表示)")
            
            if selected_models: raw_df_day = raw_df_day[raw_df_day['機種'].isin(selected_models)]
            final_df = raw_df_day[['機種', '台番', '差枚', 'G数', 'BB', 'RB', '合成', 'BIG確率', 'REG確率', '合算確率']].sort_values('差枚', ascending=False)
            st.dataframe(final_df.style.format({'G数': '{:,}', 'BIG確率': '1/{:.1f}', 'REG確率': '1/{:.1f}', '合算確率': '1/{:.1f}'}), column_config={"差枚": st.column_config.NumberColumn("差枚", format="%+d"), "機種": st.column_config.TextColumn("機種名", width="medium")}, height=400, use_container_width=True)
            total_diff = int(final_df['差枚'].sum())
            st.caption(f"📊 表示中の合計: {len(final_df)}台 / 総差枚: {total_diff:+d}枚")
        else:
            st.info("データがありません")

    with st.expander("ℹ️ アイコンの意味・判定ルール (クリックで開閉)", expanded=True):
        st.markdown("""
        <div style="font-size: 0.9rem; line-height: 1.8;">
            <b>★ 全勝/鉄板</b>: 勝率 100% かつ 平均G数 7,000G以上<br>
            <b>◎ 絶好調</b>: 勝率 66%以上 かつ 差枚+1,500枚 かつ 平均G数 7,000G以上<br>
            <b>🌀 ぶん回し</b>: 機種平均 7,000G以上<br>
            <b>○ 好調</b>: 勝率 50%以上 かつ 勝ち台平均 7,000G以上<br>
        </div>""", unsafe_allow_html=True)
    
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
    table_headers = '<thead><tr><th class="td-date">日付</th><th class="td-total">総差枚</th><th class="td-avg">平均</th><th class="td-g">平均G</th><th class="td-models">主力機種・メモ</th></tr></thead>'
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
        
        win_machines = df_day[df_day['差枚'] > 0]
        win_g_means = win_machines.groupby('機種')['G数'].mean() if not win_machines.empty else pd.Series(dtype=float)
        model_stats = df_day.groupby('機種', observed=False).agg(平均差枚=('差枚', 'mean'), 勝率=('差枚', lambda x: (x > 0).mean()), 平均G数=('G数', 'mean'), 台数=('台番', 'count')).reset_index()
        
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
                models_html_parts.append(f"<span class='model-line'>{icon} {m_name}({int(row['平均差枚']):+})</span>")
                displayed_models.add(m_name)

        models_html = "".join(models_html_parts) if models_html_parts else "-"
        table_rows += f'<tr><td class="td-date">{date_str}</td><td class="td-total {total_cls}">{total_diff:+,}</td><td class="td-avg {avg_cls}">{avg_diff:+,}</td><td class="td-g">{avg_g:,}</td><td class="td-models">{models_html}</td></tr>'

    if len(display_dates) > 0: st.markdown(f'<table class="custom-table">{table_headers}<tbody>{table_rows}</tbody></table>', unsafe_allow_html=True)
    else: st.info("表示できるデータがありません")

with tab2:
    st.subheader("🔥 店長推し分析")
    if not df_all.empty:
        stats = df_all.groupby('機種', observed=False).agg(平均差枚=('差枚', 'mean'), 勝率=('差枚', lambda x: (x>0).mean()*100), 平均G数=('G数', 'mean'), サンプル数=('台番', 'count'), 合計差枚=('差枚', 'sum')).reset_index()
        valid = stats[stats['サンプル数'] >= 5]
        if not valid.empty:
            fig = px.scatter(valid, x="勝率", y="平均差枚", size="平均G数", color="合計差枚", hover_name="機種", text="機種", color_continuous_scale=['blue', 'white', 'red'], range_color=[-50000, 50000], size_max=50)
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(valid.sort_values('平均差枚', ascending=False), use_container_width=True)

with tab3:
    st.subheader("🕵️‍♀️ 不発・塊検知")
    unlucky = df_all[(df_all['G数']>=5000) & (df_all['差枚']<=-500) & (df_all['REG確率']<=350)]
    if not unlucky.empty: st.error("不発ジャグラー候補"); st.dataframe(unlucky[['日付', '機種', '台番', '差枚', 'G数', 'RB', 'REG確率']], use_container_width=True)
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
    st.header("🔍 鉄板台サーチ＆多角分析")
    target_src = st.radio("データソース", ["現在選択中の期間 (サイドバー)", "全期間 (読込済データ)"], horizontal=True)
    base_df = df_all_raw.copy() if "全期間" in target_src else df_all.copy()
    def calc_p(g, c): return round(g/c, 1) if c>0 else 9999.0
    base_df['BIG確率'] = base_df.apply(lambda x: calc_p(x['G数'], x['BB']), axis=1)
    base_df['REG確率'] = base_df.apply(lambda x: calc_p(x['G数'], x['RB']), axis=1)
    base_df['合算確率'] = base_df.apply(lambda x: calc_p(x['G数'], x['BB']+x['RB']), axis=1)

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
            st.plotly_chart(fig, use_container_width=True)
            st.markdown("##### 🥧 機種別シェア")
            pie = px.pie(res['機種'].value_counts().reset_index(), values='count', names='機種', hole=0.4)
            st.plotly_chart(pie, use_container_width=True)
            
        st.markdown("##### 📋 エース台番リスト")
        st.dataframe(stats.head(20).style.format({'平均差枚':'{:.0f}'}), use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
        st.subheader("📝 抽出データ全リスト")
        st.dataframe(res[['日付','機種','台番','差枚','G数','合算確率','BIG確率','REG確率']].sort_values('差枚', ascending=False).style.format({'日付':'{:%Y-%m-%d}','差枚':'{:+d}'}), use_container_width=True)
    else:
        st.warning("条件に合う台はありません")