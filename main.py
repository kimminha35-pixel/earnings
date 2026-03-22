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

# [요청 종목 한정 리스트]
TICKER_NAMES = {
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
    "002270": "화밍 전력 설비", "601138": "폭스콘", "002371": "나우라 테크놀로지", 
    "300274": "선그로우", "002518": "심천 KSTAR 과학기술"
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
    requests.post(url, data={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"})

# --- 메인 실행 ---
if __name__ == "__main__":
    current_state = load_state()
    completed_list = []
    upcoming_list = []
    new_reports = []

    # 1. 모든 종목 스케줄 파악 (캘린더용)
    for ticker in YF_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            ed = stock.earnings_dates
            if ed is not None and not ed.empty:
                for date_idx, row in ed.iterrows():
                    dt = date_idx.tz_localize(None)
                    if dt.year == KST_NOW.year and dt.month == KST_NOW.month:
                        date_str = dt.strftime('%m/%d')
                        name = TICKER_NAMES.get(ticker, ticker)
                        item = f"{name} ({ticker}) - {date_str}"
                        if dt < KST_NOW.replace(tzinfo=None):
                            if item not in completed_list: completed_list.append(item)
                        else:
                            if item not in upcoming_list: upcoming_list.append(item)
            
            # 신규 실적 데이터 체크 (어제 브리핑 이후 새로 올라온 것)
            q_fin = stock.quarterly_financials
            if not q_fin.empty:
                l_date_str = q_fin.columns[0].strftime('%Y-%m-%d')
                if current_state.get(ticker) != l_date_str:
                    # 요약 데이터 생성 (매출, 순익, EPS 등 기존 로직)
                    try: r_curr, r_past = q_fin.loc['Total Revenue'][0], q_fin.loc['Total Revenue'][4]
                    except: r_curr, r_past = 0, 0
                    y_rev = ((r_curr - r_past) / r_past * 100) if r_past else 0
                    
                    eps_tag = ""
                    try:
                        past_d = ed[ed.index < pd.Timestamp.utcnow()]
                        if not past_d.empty:
                            est, act = past_d.iloc[0].get('Estimate'), past_d.iloc[0].get('Reported')
                            if pd.notna(est) and pd.notna(act):
                                eps_tag = " [🟢Beat]" if act > est else " [🔴Miss]" if act < est else " [⚪Meet]"
                    except: pass

                    new_reports.append(f"✅ **{TICKER_NAMES[ticker]}**{eps_tag} (YoY 매출: {y_rev:+.1f}%)")
                    current_state[ticker] = l_date_str
        except: pass

    # 2. 중국 본토 실적 체크 (CN)
    for ticker in CN_TICKERS:
        try:
            df = ak.stock_financial_analysis_indicator(symbol=ticker)
            if df.empty: continue
            d_col = '日期' if '日期' in df.columns else df.columns[0]
            l_date_str = pd.to_datetime(df.iloc[0][d_col]).strftime('%Y-%m-%d')
            if current_state.get(ticker) != l_date_str:
                new_reports.append(f"✅ **{TICKER_NAMES[ticker]} (CN)** 실적 업데이트")
                current_state[ticker] = l_date_str
        except: pass

    # 3. 통합 메시지 구성
    full_msg = f"🗓 **{KST_NOW.year}년 {KST_NOW.month}월 글로벌 실적 캘린더**\n\n"
    
    if completed_list:
        full_msg += "✅ **이번 달 완료**\n" + "\n".join([f"• {x} 완료" for x in sorted(list(set(completed_list)))]) + "\n\n"
    
    if upcoming_list:
        full_msg += "🔜 **이번 달 예정**\n" + "\n".join([f"• {x} 예정" for x in sorted(list(set(upcoming_list)))]) + "\n\n"

    if new_reports:
        full_msg += "───────────────\n"
        full_msg += "📢 **신규 실적 업데이트**\n" + "\n".join(new_reports)
    else:
        full_msg += "───────────────\n"
        full_msg += "📢 오늘 새로 발표된 실적은 없습니다."

    send_telegram(full_msg)
    save_state(current_state)
