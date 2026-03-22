import os
import csv
import requests
import pandas as pd
import yfinance as yf
import akshare as ak
from datetime import datetime, timedelta

# --- 1. 환경변수 로드 ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

# 티커 리스트 분리 (미국: 일반 / 중국: 6자리 숫자)
US_TICKERS = ["VICR", "MOG.A", "OII"]
CN_TICKERS = ["600089", "300308"] 
STATE_FILE = "earnings_state.csv"

# 시간 설정 (한국 시간 KST)
KST = datetime.utcnow() + timedelta(hours=9)
CURRENT_MONTH_STR = KST.strftime('%Y-%m') 

# --- 2. CSV 상태 관리 ---
def load_state():
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) == 2:
                    state[row[0]] = row[1]
    return state

def save_state(state):
    with open(STATE_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for key, val in state.items():
            writer.writerow([key, val])

# --- 3. 텔레그램 발송 ---
def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    requests.post(url, data=payload)

# --- 4. 어닝 스케줄 브리핑 (월 1회, 미국 주식 한정) ---
def send_monthly_schedule(state):
    if state.get('SCHEDULE_MONTH') == CURRENT_MONTH_STR:
        return False

    schedule_msg = f"🗓 **{KST.year}년 {KST.month}월 실적 발표 캘린더 (미국)**\n\n"
    completed = []
    upcoming = []

    for ticker in US_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            ed = stock.earnings_dates
            
            if ed is not None and not ed.empty:
                for date_idx, row in ed.iterrows():
                    dt = date_idx.tz_localize(None)
                    if dt.year == KST.year and dt.month == KST.month:
                        date_str = dt.strftime('%m/%d')
                        if dt < KST.replace(tzinfo=None):
                            completed.append(f"✅ {ticker} ({date_str} 완료)")
                        else:
                            upcoming.append(f"🔜 {ticker} ({date_str} 예정)")
        except Exception as e:
            print(f"Schedule Error on {ticker}: {e}")

    if completed:
        schedule_msg += "✅ **이번 달 종료**\n" + "\n".join(completed) + "\n\n"
    if upcoming:
        schedule_msg += "🔜 **이번 달 예정**\n" + "\n".join(upcoming) + "\n\n"
        
    if not completed and not upcoming:
        schedule_msg += "이번 달에 예정된 미국 주식 실적 발표가 없습니다.\n\n"
        
    schedule_msg += "*(참고: 중국 주식 일정은 무료 API 한계로 브리핑에서 제외되며, 실적 업데이트 시 즉시 알림만 제공됩니다)*"

    send_telegram(schedule_msg)
    state['SCHEDULE_MONTH'] = CURRENT_MONTH_STR
    return True

# --- 5. 미국 실적 & YoY 확인 (yfinance) ---
def check_us_earnings(state):
    updated = False
    for ticker in US_TICKERS:
        try:
            stock = yf.Ticker(ticker)
            q_financials = stock.quarterly_financials
            
            if q_financials.empty:
                continue
                
            latest_date_obj = q_financials.columns[0]
            latest_date_str = latest_date_obj.strftime('%Y-%m-%d')
            
            if state.get(ticker) == latest_date_str:
                continue
                
            if len(q_financials.columns) >= 5:
                current_rev = q_financials.loc['Total Revenue'][0]
                past_rev = q_financials.loc['Total Revenue'][4]
                yoy_rev = ((current_rev - past_rev) / past_rev) * 100 if past_rev else 0
                
                current_ni = q_financials.loc['Net Income'][0]
                past_ni = q_financials.loc['Net Income'][4]
                yoy_ni = ((current_ni - past_ni) / abs(past_ni)) * 100 if past_ni else 0
                
                msg = (
                    f"🇺🇸 **[{ticker}] 신규 실적 업데이트**\n"
                    f"🗓 기준 분기: {latest_date_str}\n\n"
                    f"📊 **매출 (Revenue)**\n"
                    f"• $ {current_rev:,.0f} (YoY: {yoy_rev:+.1f}%)\n\n"
                    f"💰 **순이익 (Net Income)**\n"
                    f"• $ {current_ni:,.0f} (YoY: {yoy_ni:+.1f}%)\n"
                )
                
                send_telegram(msg)
                state[ticker] = latest_date_str
                updated = True
        except Exception as e:
            print(f"US Error on {ticker}: {e}")
            
    return updated

# --- 6. 중국 실적 & YoY 확인 (akshare) ---
def check_cn_earnings(state):
    updated = False
    for ticker in CN_TICKERS:
        try:
            # 단일 종목 재무지표 함수로 변경
            df = ak.stock_financial_analysis_indicator(symbol=ticker)
            if df.empty:
                continue
                
            # 날짜 컬럼 찾기 (기본 '日期')
            date_col = '日期' if '日期' in df.columns else df.columns[0]
            
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.sort_values(by=date_col, ascending=False).reset_index(drop=True)
            
            latest_date_obj = df.iloc[0][date_col]
            latest_date_str = latest_date_obj.strftime('%Y-%m-%d')
            
            if state.get(ticker) == latest_date_str:
                continue
                
            # 매출, 순이익 컬럼 유연하게 찾기
            rev_col = next((c for c in df.columns if '主营业务收入' in c or '营业收入' in c or '收入' in c), None) 
            ni_col = next((c for c in df.columns if '净利润' in c or '利润' in c), None)
            
            if not rev_col or not ni_col:
                print(f"CN Error on {ticker}: 필수 컬럼 누락 -> {list(df.columns)}")
                continue

            current_rev = float(df.iloc[0][rev_col])
            current_ni = float(df.iloc[0][ni_col])
            
            # 1년 전 데이터 매칭 (YoY 계산용)
            past_date_str = f"{latest_date_obj.year - 1}-{latest_date_obj.month:02d}-{latest_date_obj.day:02d}"
            past_df = df[df[date_col] == pd.to_datetime(past_date_str)]
            
            yoy_rev = 0
            yoy_ni = 0
            
            if not past_df.empty:
                past_rev = float(past_df.iloc[0][rev_col])
                past_ni = float(past_df.iloc[0][ni_col])
                yoy_rev = ((current_rev - past_rev) / past_rev) * 100 if past_rev else 0
                yoy_ni = ((current_ni - past_ni) / abs(past_ni)) * 100 if past_ni else 0
                
            msg = (
                f"🇨🇳 **[{ticker}] 신규 실적 업데이트**\n"
                f"🗓 기준 분기: {latest_date_str}\n\n"
                f"📊 **매출 (Total Revenue)**\n"
                f"• ¥ {current_rev:,.0f} (YoY: {yoy_rev:+.1f}%)\n\n"
                f"💰 **순이익 (Net Income)**\n"
                f"• ¥ {current_ni:,.0f} (YoY: {yoy_ni:+.1f}%)\n"
            )
            send_telegram(msg)
            state[ticker] = latest_date_str
            updated = True
            
        except Exception as e:
            print(f"CN Error on {ticker}: {e}")
            
    return updated

# --- 메인 실행부 ---
if __name__ == "__main__":
    current_state = load_state()
    
    # 1. 스케줄 브리핑 체크
    schedule_updated = send_monthly_schedule(current_state)
    
    # 2. 신규 실적 체크
    us_updated = check_us_earnings(current_state)
    cn_updated = check_cn_earnings(current_state)
    
    # 변경사항이 있으면 CSV 저장
    if schedule_updated or us_updated or cn_updated:
        save_state(current_state)
        print("상태 CSV 파일 업데이트 완료")
    else:
        print("새로운 업데이트 없음.")
