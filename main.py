import os
import csv
import requests
import pandas as pd
import yfinance as yf
import akshare as ak
from datetime import datetime, timedelta

# --- 1. 환경변수 및 설정 ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
STATE_FILE = "earnings_state.csv"

# [요청하신 종목 한정 리스트]
TICKER_NAMES = {
    # 미국 및 기타 글로벌 (yfinance)
    "GOOGL": "알파벳", "TSM": "타이완 반도체 제조 회사", "NVDA": "엔비디아", "GLW": "코닝", 
    "TSLA": "테슬라", "AMZN": "아마존", "RTX": "RTX 코프", "COHR": "코히어런트", 
    "VRT": "버티브", "AKAM": "아카마이", "DIS": "디즈니", "C": "CITI", 
    "OII": "오셔니어링", "VTRS": "비아트리스", "WFRD": "웨더포드",
    "9988.HK": "알리바바 그룹", "3750.HK": "CATL", "0700.HK": "텐센트홀딩스", "1072.HK": "동방전기", 
    "2590.HK": "긱플러스", "9868.HK": "샤오펑", "1133.HK": "하얼빈동력", "0981.HK": "SMIC", 
    "2318.HK": "중국평안보험", "6030.HK": "중신증권유한회사", "0883.HK": "중국해양석유유한공사", 
    "1378.HK": "중국굉교", "1288.HK": "중국농업은행", "0392.HK": "베이징 엔터프라이즈", 
    "1800.HK": "중국교통건설", "0579.HK": "북경경능청정에너지", "2899.HK": "자금 광업", 
    "1866.HK": "중국 XLX 비료", "4901.T": "후지필름", "6981.T": "무라타", 
    "005930.KS": "삼성전자", "000660.KS": "SK하이닉스", "012330.KS": "현대모비스", 
    "005387.KS": "현대차2우B", "005380.KS": "현대차", "071050.KS": "한국금융지주", "001040.KS": "CJ",
    # 중국 본토 (akshare용 6자리)
    "002270": "화밍 전력 설비", "601138": "폭스콘", "002371": "나우라 테크놀로지", 
    "300274": "선그로우", "002518": "심천 KSTAR 과학기술"
}

YF_TICKERS = [k for k in TICKER_NAMES.keys() if "." in k or k.isalpha()]
CN_TICKERS = [k for k in TICKER_NAMES.keys() if k.isdigit() and len(k) == 6]

KST_NOW = datetime.utcnow() + timedelta(hours=9)
CURRENT_MONTH_STR = KST_NOW.strftime('%Y-%m')

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
    requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"})

def get_currency_symbol(ticker):
    if ".HK" in ticker: return "HK$"
    elif ".T" in ticker: return "¥"
    elif ".KS" in ticker: return "₩"
    else: return "$"

# --- 메인 실행 ---
if __name__ == "__main__":
    current_state = load_state()
    report_content = []
    
    # 1. 글로벌 (yfinance)
    for ticker in YF_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            q_fin = stock.quarterly_financials
            if q_fin.empty: continue
            
            l_date_str = q_fin.columns[0].strftime('%Y-%m-%d')
            if current_state.get(ticker) == l_date_str: continue
            
            # 매출/순익 YoY
            try: r_curr, r_past = q_fin.loc['Total Revenue'][0], q_fin.loc['Total Revenue'][4]
            except: r_curr, r_past = q_fin.loc['Operating Revenue'][0], q_fin.loc['Operating Revenue'][4]
            y_rev = ((r_curr - r_past) / r_past * 100) if r_past else 0
            
            n_curr, n_past = q_fin.loc['Net Income'][0], q_fin.loc['Net Income'][4]
            y_ni = ((n_curr - n_past) / abs(n_past) * 100) if n_past else 0
            
            # EPS 서프라이즈 및 실제 발표일
            eps_tag, rel_time = "", ""
            try:
                ed = stock.earnings_dates
                past_d = ed[ed.index < pd.Timestamp.utcnow()]
                if not past_d.empty:
                    latest = past_d.iloc[0]
                    rel_time = past_d.index[0].tz_convert('Asia/Seoul').strftime('%m/%d %H:%M')
                    est, act = latest.get('Estimate'), latest.get('Reported')
                    if pd.notna(est) and pd.notna(act):
                        eps_tag = " [🟢Beat]" if act > est else " [🔴Miss]" if act < est else " [⚪Meet]"
            except: pass

            name, cur = TICKER_NAMES[ticker], get_currency_symbol(ticker)
            line = (f"📍 **{name}** ({ticker}){eps_tag}\n"
                    f"   • 발표: {rel_time} | 분기: {l_date_str}\n"
                    f"   • 매출: {cur}{r_curr:,.0f} ({y_rev:+.1f}%)\n"
                    f"   • 순익: {cur}{n_curr:,.0f} ({y_ni:+.1f}%)")
            report_content.append(line)
            current_state[ticker] = l_date_str
        except: pass

    # 2. 중국 본토 (akshare)
    for ticker in CN_TICKERS:
        try:
            df = ak.stock_financial_analysis_indicator(symbol=ticker)
            if df.empty: continue
            d_col = '日期' if '日期' in df.columns else df.columns[0]
            df[d_col] = pd.to_datetime(df[d_col])
            df = df.sort_values(by=d_col, ascending=False).reset_index(drop=True)
            l_date_str = df.iloc[0][d_col].strftime('%Y-%m-%d')
            if current_state.get(ticker) == l_date_str: continue
            
            r_c = next((c for c in df.columns if any(k in c for k in ['营业收入', '收入'])), df.columns[1])
            n_c = next((c for c in df.columns if '净利润' in c), df.columns[2])
            
            curr_r, curr_n = float(df.iloc[0][r_c]), float(df.iloc[0][n_c])
            p_date = df.iloc[0][d_col] - pd.DateOffset(years=1)
            p_df = df[df[d_col] == p_date]
            
            y_r, y_n = 0, 0
            if not p_df.empty:
                past_r, past_n = float(p_df.iloc[0][r_c]), float(p_df.iloc[0][n_c])
                y_r = (curr_r - past_r) / past_r * 100 if past_r else 0
                y_n = (curr_n - past_n) / abs(past_n) * 100 if past_n else 0
                
            name = TICKER_NAMES[ticker]
            line = (f"📍 **{name}** ({ticker}) [CN]\n"
                    f"   • 분기: {l_date_str}\n"
                    f"   • 매출: ¥{curr_r:,.0f} ({y_r:+.1f}%)\n"
                    f"   • 순익: ¥{curr_n:,.0f} ({y_n:+.1f}%)")
            report_content.append(line)
            current_state[ticker] = l_date_str
        except: pass

    # 3. 전송
    if report_content:
        msg = f"☀️ **{KST_NOW.strftime('%m/%d')} 실적 요약 브리핑**\n\n" + "\n\n".join(report_content)
        send_telegram(msg)
        save_state(current_state)
