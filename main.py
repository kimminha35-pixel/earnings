import os
import csv
import requests
import pandas as pd
import yfinance as yf
import akshare as ak
from datetime import datetime, timedelta

# --- 1. 설정 및 종목 리스트 ---
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
    # 메시지가 너무 길면 쪼개서 보냄
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

# --- 메인 실행 ---
if __name__ == "__main__":
    current_state = load_state()
    new_reports = []

    # 1. 글로벌 (yfinance)
    for ticker in YF_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            q_fin = stock.quarterly_financials
            if q_fin.empty: continue
            
            l_date = q_fin.columns[0].strftime('%Y-%m-%d')
            if current_state.get(ticker) == l_date: continue
            
            # 매출, 영익(EBIT) 추출
            try: r_c, r_p = q_fin.loc['Total Revenue'][0], q_fin.loc['Total Revenue'][4]
            except: r_c, r_p = 0, 0
            try: o_c, o_p = q_fin.loc['EBIT'][0], q_fin.loc['EBIT'][4]
            except: o_c, o_p = 0, 0
            
            y_r = ((r_c - r_p) / r_p * 100) if r_p else 0
            y_o = ((o_c - o_p) / abs(o_p) * 100) if o_p else 0
            
            # 발표시간 & EPS
            rel_time, eps_tag = "N/A", ""
            try:
                ed = stock.earnings_dates
                past = ed[ed.index < pd.Timestamp.utcnow()]
                if not past.empty:
                    rel_time = past.index[0].tz_convert('Asia/Seoul').strftime('%m/%d %H:%M')
                    est, act = past.iloc[0].get('Estimate'), past.iloc[0].get('Reported')
                    if pd.notna(est) and pd.notna(act):
                        eps_tag = "🟢" if act > est else "🔴" if act < est else "⚪"
            except: pass

            name = TICKER_NAMES[ticker]
            report = (f"📍 *{name}* ({ticker}) {eps_tag}\n"
                      f"📅 발표: {rel_time} (KST)\n"
                      f"💰 매출: {format_num(r_c)} (YoY {y_r:+.1f}%)\n"
                      f"📉 영익: {format_num(o_c)} (YoY {y_o:+.1f}%)\n")
            new_reports.append(report)
            current_state[ticker] = l_date
        except: pass

    # 2. 중국 (akshare)
    for ticker in CN_TICKERS:
        try:
            df = ak.stock_financial_analysis_indicator(symbol=ticker)
            if df.empty: continue
            d_col = '日期' if '日期' in df.columns else df.columns[0]
            l_date = pd.to_datetime(df.iloc[0][d_col]).strftime('%Y-%m-%d')
            if current_state.get(ticker) == l_date: continue
            
            r_c = float(df.iloc[0].get('营业收入', 0))
            o_c = float(df.iloc[0].get('营业利润', 0))
            
            # YoY 계산
            p_date = pd.to_datetime(df.iloc[0][d_col]) - pd.DateOffset(years=1)
            p_df = df[df[d_col] == p_date]
            y_r, y_o = 0, 0
            if not p_df.empty:
                r_p, o_p = float(p_df.iloc[0].get('营业收入', 0)), float(p_df.iloc[0].get('营业利润', 0))
                y_r = (r_c - r_p) / r_p * 100 if r_p else 0
                y_o = (o_c - o_p) / abs(o_p) * 100 if o_p else 0

            name = TICKER_NAMES[ticker]
            report = (f"📍 *{name}* ({ticker}) [CN]\n"
                      f"📅 분기: {l_date}\n"
                      f"💰 매출: ¥{format_num(r_c)} (YoY {y_r:+.1f}%)\n"
                      f"📉 영익: ¥{format_num(o_c)} (YoY {y_o:+.1f}%)\n")
            new_reports.append(report)
            current_state[ticker] = l_date
        except: pass

    # 3. 통합 전송
    if new_reports:
        msg = f"☀️ *{KST_NOW.strftime('%m/%d')} 실적 디테일 요약*\n"
        msg += "───────────────────\n\n"
        msg += "\n".join(new_reports)
        send_telegram(msg)
        save_state(current_state)
    else:
        print("신규 실적 없음")
