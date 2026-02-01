import os
import datetime
import json
import pandas as pd
import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# ================= 設定區 =================
SITE_URL = 'https://chengann520.github.io/Chengann/'
SHEET_NAME = 'GSC_Data_Auto'  # Google 試算表檔名
RAW_SHEET = 'Raw_Data'        # 存放關鍵字細節的分頁
TOTAL_SHEET = 'Daily_Total'   # 存放每日總量的分頁
DEVICE_SHEET = 'Device_Data'  # 存放裝置資料的分頁
QUERY_SHEET = 'Query_Data'    # 存放每日關鍵字的小計分頁
# =========================================

# 定義權限範圍 (GSC + Sheets + Drive)
SCOPES = [
    'https://www.googleapis.com/auth/webmasters.readonly',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_credentials():
    """ 優先讀取環境變數，次之讀取本地檔案 """
    if 'GCP_CREDENTIALS' in os.environ:
        creds_info = json.loads(os.environ['GCP_CREDENTIALS'])
        return service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    elif os.path.exists('credentials.json'):
        return service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    else:
        return None

def get_gspread_client():
    """ 取得 gspread 授權客戶端 """
    creds = get_credentials()
    if not creds:
        return None
    return gspread.authorize(creds)

def ensure_worksheet(spreadsheet, sheet_name, headers):
    """ 確保分頁存在且有標題列 """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f">> 建立新分頁: {sheet_name}")
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
    
    # 檢查標題列
    current_headers = worksheet.row_values(1)
    if not current_headers:
        print(f">> 寫入 {sheet_name} 標題列...")
        worksheet.append_row(headers)
    return worksheet

def get_last_date(worksheet):
    """ 從試算表取得最後一筆資料的日期 """
    try:
        dates = worksheet.col_values(1)[1:]
        if not dates:
            return None
        return max(dates)
    except Exception as e:
        print(f">> 讀取 {worksheet.title} 最後日期失敗: {e}")
        return None

def fetch_gsc_data():
    print(f"[{datetime.datetime.now()}] 開始執行 GSC v4.0 資料抓取任務...")
    
    creds = get_credentials()
    if not creds:
        print("錯誤：找不到 GCP_CREDENTIALS 或 credentials.json")
        return
    
    client = get_gspread_client()
    if not client:
        return
    
    try:
        sh = client.open(SHEET_NAME)
        # 初始化工作表
        raw_ws = ensure_worksheet(sh, RAW_SHEET, ['date', 'query', 'page', 'clicks', 'impressions', 'ctr', 'position'])
        total_ws = ensure_worksheet(sh, TOTAL_SHEET, ['date', 'clicks', 'impressions', 'ctr', 'position'])
        device_ws = ensure_worksheet(sh, DEVICE_SHEET, ['date', 'device', 'clicks', 'impressions', 'ctr', 'position'])
        query_ws = ensure_worksheet(sh, QUERY_SHEET, ['date', 'query', 'clicks', 'impressions', 'ctr', 'position'])
    except Exception as e:
        print(f"錯誤：無法開啟試算表 '{SHEET_NAME}'。詳細內容: {e}")
        return

    service = build('searchconsole', 'v1', credentials=creds)

    # 以 Daily_Total 的日期作為基準
    last_date_str = get_last_date(total_ws)
    today = datetime.date.today()
    end_date = today - datetime.timedelta(days=3)

    if last_date_str is None:
        start_date = today - datetime.timedelta(days=400)
        print(">> 首次執行，抓取歷史資料...")
    else:
        last_date_obj = datetime.datetime.strptime(last_date_str, '%Y-%m-%d').date()
        start_date = last_date_obj + datetime.timedelta(days=1)
        print(f">> 從 {start_date} 開始更新...")

    if start_date > end_date:
        print(">> 資料已是最新。")
        return

    date_range = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d')
    }

    try:
        # 1. 抓取 Daily_Total (僅依日期分組)
        print(f">> 正在抓取 {TOTAL_SHEET} 總量資料...")
        req_total = {**date_range, 'dimensions': ['date']}
        resp_total = service.searchanalytics().query(siteUrl=SITE_URL, body=req_total).execute()
        rows_total = resp_total.get('rows', [])
        
        if rows_total:
            data_total = [[r['keys'][0], r['clicks'], r['impressions'], r['ctr'], r['position']] for r in rows_total]
            total_ws.append_rows(data_total)
            print(f"   - 成功存入 {len(data_total)} 筆總量資料。")

        # 2. 抓取 Raw_Data (依 日期/關鍵字/網頁 分組)
        print(f">> 正在抓取 {RAW_SHEET} 細節資料...")
        req_raw = {**date_range, 'dimensions': ['date', 'query', 'page'], 'rowLimit': 25000}
        resp_raw = service.searchanalytics().query(siteUrl=SITE_URL, body=req_raw).execute()
        rows_raw = resp_raw.get('rows', [])

        if rows_raw:
            data_raw = [[r['keys'][0], r['keys'][1], r['keys'][2], r['clicks'], r['impressions'], r['ctr'], r['position']] for r in rows_raw]
            raw_ws.append_rows(data_raw)
            print(f"   - 成功存入 {len(data_raw)} 筆細節資料。")
        else:
            print(">> ⚠️ 本次查詢無詳細關鍵字資料 (可能是流量低或隱私過濾)，Google 回傳空列表。")

        # 3. 抓取 Device_Data (依 日期/裝置 分組)
        print(f">> 正在抓取 {DEVICE_SHEET} 裝置資料...")
        req_device = {**date_range, 'dimensions': ['date', 'device']}
        resp_device = service.searchanalytics().query(siteUrl=SITE_URL, body=req_device).execute()
        rows_device = resp_device.get('rows', [])
        
        if rows_device:
            data_device = [[r['keys'][0], r['keys'][1], r['clicks'], r['impressions'], r['ctr'], r['position']] for r in rows_device]
            device_ws.append_rows(data_device)
            print(f"   - 成功存入 {len(data_device)} 筆裝置資料。")

        # 4. 抓取 Query_Data (依 日期/關鍵字 分組)
        print(f">> 正在抓取 {QUERY_SHEET} 關鍵字資料...")
        req_query = {**date_range, 'dimensions': ['date', 'query']}
        resp_query = service.searchanalytics().query(siteUrl=SITE_URL, body=req_query).execute()
        rows_query = resp_query.get('rows', [])
        
        if rows_query:
            data_query = [[r['keys'][0], r['keys'][1], r['clicks'], r['impressions'], r['ctr'], r['position']] for r in rows_query]
            query_ws.append_rows(data_query)
            print(f"   - 成功存入 {len(data_query)} 筆關鍵字資料。")
            
    except Exception as e:
        print(f">> GSC 資料抓取發生錯誤: {e}")

def fetch_ga4_data():
    """ GA4 資料抓取預留位置 """
    print(f"[{datetime.datetime.now()}] (預留) 開始執行 GA4 資料抓取任務...")
    pass

if __name__ == "__main__":
    fetch_gsc_data()
    fetch_ga4_data()
