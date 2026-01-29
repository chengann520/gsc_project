import os
import datetime
import json
import pandas as pd
import sqlite3
from google.oauth2 import service_account
from googleapiclient.discovery import build
# 預留 GA4 支援
# from google.analytics.data_v1beta import BetaAnalyticsDataClient

# ================= 設定區 =================
SITE_URL = 'https://chengann.github.io/'
DB_NAME = 'gsc_data.db'
TABLE_NAME = 'search_performance'
# =========================================

def get_db_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    # GSC 資料表
    c.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date TEXT,
            query TEXT,
            page TEXT,
            clicks INTEGER,
            impressions INTEGER,
            ctr REAL,
            position REAL,
            PRIMARY KEY (date, query, page)
        )
    ''')
    
    # Date Table (日期維度表，方便 Power BI 時間分析)
    c.execute('''
        CREATE TABLE IF NOT EXISTS dim_date (
            date TEXT PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            weekday INTEGER,
            week_of_year INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    update_date_table()

def update_date_table():
    """ 根據數據範圍自動填充日期表 """
    conn = get_db_connection()
    start_date = datetime.date.today() - datetime.timedelta(days=400)
    end_date = datetime.date.today() + datetime.timedelta(days=7) # 預留未來幾天
    
    dates = pd.date_range(start_date, end_date)
    df_date = pd.DataFrame({'date': dates.strftime('%Y-%m-%d')})
    df_date['year'] = dates.year
    df_date['month'] = dates.month
    df_date['day'] = dates.day
    df_date['weekday'] = dates.weekday
    df_date['week_of_year'] = dates.isocalendar().week
    
    df_date.to_sql('dim_date', conn, if_exists='replace', index=False)
    conn.close()
    print(">> 日期表 (dim_date) 已更新。")

def get_last_date():
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
        result = c.fetchone()[0]
    except:
        result = None
    conn.close()
    return result

def get_credentials():
    """ 優先讀取環境變數，次之讀取本地檔案 """
    if 'GCP_CREDENTIALS' in os.environ:
        creds_json = json.loads(os.environ['GCP_CREDENTIALS'])
        return service_account.Credentials.from_service_account_info(creds_json)
    elif os.path.exists('credentials.json'):
        return service_account.Credentials.from_service_account_file('credentials.json')
    else:
        return None

def fetch_gsc_data():
    print(f"[{datetime.datetime.now()}] 開始執行 GSC 資料抓取任務...")
    
    creds = get_credentials()
    if not creds:
        print("錯誤：找不到 GCP_CREDENTIALS 環境變數或 credentials.json")
        return
    
    service = build('searchconsole', 'v1', credentials=creds)
    # ----------------------------------

    last_date_in_db = get_last_date()
    today = datetime.date.today()
    end_date = today - datetime.timedelta(days=3)

    if last_date_in_db is None:
        start_date = today - datetime.timedelta(days=400)
        print(">> 首次執行，抓取歷史資料...")
    else:
        last_date_obj = datetime.datetime.strptime(last_date_in_db, '%Y-%m-%d').date()
        start_date = last_date_obj + datetime.timedelta(days=1)
        print(f">> 從 {start_date} 開始更新...")

    if start_date > end_date:
        print(">> 資料已是最新。")
        return

    request = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d'),
        'dimensions': ['date', 'query', 'page'],
        'rowLimit': 25000 
    }

    try:
        response = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        rows = response.get('rows', [])
        
        if rows:
            data_list = []
            for row in rows:
                data_list.append({
                    'date': row['keys'][0],
                    'query': row['keys'][1],
                    'page': row['keys'][2],
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'],
                    'position': row['position']
                })
            
            df = pd.DataFrame(data_list)
            conn = get_db_connection()
            df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
            conn.close()
            print(f">> 成功存入 {len(df)} 筆資料！")
        else:
            print(">> 無新資料。")
            
    except Exception as e:
        print(f">> GSC 資料抓取發生錯誤: {e}")

def fetch_ga4_data():
    """ GA4 資料抓取預留位置 """
    print(f"[{datetime.datetime.now()}] (預留) 開始執行 GA4 資料抓取任務...")
    # 未來可在此實作 GA4 Data API 呼叫
    pass

if __name__ == "__main__":
    init_db()
    fetch_gsc_data()
    fetch_ga4_data()
