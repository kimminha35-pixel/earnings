import os
import csv
import requests
import pandas as pd
import yfinance as yf
import akshare as ak
from datetime import datetime, timedelta

# --- 1. 설정 및 종목 리스트 (생략 없이 유지) ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
STATE_FILE = "earnings_state.csv"

TICKER_NAMES = {
    "GOOGL": "알파벳", "TSM": "TSMC", "NVDA": "엔비디아", "GLW": "코닝", 
    "TSLA": "테슬라", "AMZN": "아마존", "RTX": "RTX 코프", "COHR": "코히어런트", 
    "VRT": "버티브", "AKAM": "아카마이", "DIS": "디즈니", "C": "CITI", 
    "OII": "오셔니어링", "VTRS": "비아트리스", "WFRD": "웨더포드",
    "9988.HK": "알리바바", "3750.HK": "CATL", "0700.HK": "텐센트", "1072.HK": "동방전기", 
    "2590.HK": "긱플러스", "9868.HK": "샤오펑", "1133.HK": "하얼빈동력", "0981.HK": "SMIC", 
    "2318.HK": "중국평안보험", "6030.HK": "중신증권", "0883.HK": "CNOOC", 
    "1378.HK": "중국굉교", "1288.HK": "농업은행", "0392.HK": "북경엔터", 
    "1800.HK": "중국교통건설", "0579.HK": "경능청정", "2899.HK": "자금광업", 
    "1866.HK": "XLX비료", "4901.T": "후지필름", "6981.T": "무라타", 
    "005930.KS": "삼성전자", "000660.KS": "SK하이닉스", "012330.KS": "현대모비스", 
    "005387.KS": "현대차2우B", "005380.KS": "현대차", "071050.KS": "한국금융", "001040.KS": "CJ",
    "002270": "화밍전력", "601138": "폭스콘", "002371": "나우라", 
    "300274": "선그로우", "002518": "KSTAR"
}

YF_TICKERS = [k for k in TICKER_NAMES.keys() if "." in k or k.isalpha()]
CN_TICKERS = [k for k in TICKER_NAMES.keys() if k.isdigit() and len(k) == 6]
KST_NOW = datetime.utcnow() + timedelta(hours=9)

def load_state():
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) == 2: state[row[0]] = row[1]
    return state

def save_state(state):
    with open(STATE_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for key, val in state.items(): writer.writerow([key, val])

def send_telegram(message):
    if not message: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    if len(message) > 4000:
        parts = [message[i:i+4000] for i in range(0, len(message), 4000)]
        for part in parts:
            requests.post(url, data={"chat_id": CHAT_ID, "text": part, "parse_mode": "Markdown"})
    else:
        requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"})

def format_num(num):
    if pd.isna(num) or num == 0: return "N/A"
    if abs(num) >= 1e12: return f"{num/1e12:.1f}T"
    if abs(num) >= 1e9: return f"{num/1e9:.1f}B"
    if abs(num) >= 1e6: return f"{num/1e6:.1f}M"
    return f"{num:,.0f}"

if __name__ == "__main__":
    current_state = load_state()
    completed_list, upcoming_list, new_reports = [], [], []

    # 1. 글로벌 (yfinance)
    for ticker in YF_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            ed = stock.earnings_dates
            if ed is not None and not ed.empty:
                for date_idx, row in ed.iterrows():
                    dt = date_idx.tz_localize(None)
                    if dt.year == KST_NOW.year and dt.month == KST_NOW.month:
                        item = f"{TICKER_NAMES[ticker]} ({ticker}) - {dt.strftime('%m/%d')}"
                        if dt < KST_NOW.replace(tzinfo=None): completed_list.append(item)
                        else: upcoming_list.append(item)
            
            q_fin = stock.quarterly_financials
            if not q_fin.empty:
                l_date = q_fin.columns[0].strftime('%Y-%m-%d')
                if current_state.get(ticker) != l_date:
                    try: r_c, r_p = q_fin.loc['Total Revenue'][0], q_fin.loc['Total Revenue'][4]
                    except: r_c, r_p = 0, 0
                    try: o_c, o_p = q_fin.loc['EBIT'][0], q_fin.loc['EBIT'][4]
                    except: o_c, o_p = 0, 0
                    y_r = ((r_c - r_p) / r_p * 100) if r_p else 0
                    y_o = ((o_c - o_p) / abs(o_p) * 100) if o_p else 0
                    
                    rel_time, eps_tag = "N/A", ""
                    try:
                        past = ed[ed.index < pd.Timestamp.utcnow()]
                        if not past.empty:
                            rel_time = past.index[0].tz_convert('Asia/Seoul').strftime('%m/%d %H:%M')
                            est, act = past.iloc[0].get('Estimate'), past.iloc[0].get('Reported')
                            if pd.notna(est) and pd.notna(act):
                                eps_tag = "🟢" if act > est else "🔴" if act < est else "⚪"
                    except: pass

                    report = (f"📍 *{TICKER_NAMES[ticker]}* {eps_tag}\n"
                              f"📅 발표: {rel_time} (KST)\n"
                              f"💰 매출: {format_num(r_c)} (YoY {y_r:+.1f}%)\n"
                              f"📉 영익: {format_num(o_c)} (YoY {y_o:+.1f}%)")
                    new_reports.append(report)
                    current_state[ticker] = l_date
        except: pass

    # 2. 중국 (akshare) - 요약 리스트만
    for ticker in CN_TICKERS:
        try:
            df = ak.stock_financial_analysis_indicator(symbol=ticker)
            if df.empty: continue
            l_date = pd.to_datetime(df.iloc[0]['日期']).strftime('%Y-%m-%d')
            if current_state.get(ticker) != l_date:
                new_reports.append(f"📍 *{TICKER_NAMES[ticker]}* (CN) 실적 업데이트")
                current_state[ticker] = l_date
        except: pass

    # --- 3. 통합 메시지 구성 (조건 없이 발송) ---
    msg = f"🗓 *{KST_NOW.year}년 {KST_NOW.month}월 실적 브리핑 ({KST_NOW.strftime('%m/%d')})*\n\n"
    
    if completed_list:
        msg += "✅ *이번 달 완료*\n" + "\n".join([f"• {x} 완료" for x in sorted(list(set(completed_list)))]) + "\n\n"
    if upcoming_list:
        msg += "🔜 *이번 달 예정*\n" + "\n".join([f"• {x} 예정" for x in sorted(list(set(upcoming_list)))]) + "\n\n"

    msg += "───────────────\n"
    if new_reports:
        msg += "📢 *신규 실적 업데이트*\n\n" + "\n\n".join(new_reports)
    else:
        msg += "📢 오늘 새로 확인된 실적은 없습니다."

    send_telegram(msg)
    save_state(current_state)
