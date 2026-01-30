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
SHEET_NAME = 'GSC_Data_Auto'  # Google 試算表名稱
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

def init_sheet():
    """ 初始化試算表，如果沒有標題列則建立 """
    client = get_gspread_client()
    if not client:
        print("錯誤：無法取得 Google Sheets 授權")
        return None
    
    try:
        sh = client.open(SHEET_NAME)
        worksheet = sh.get_worksheet(0)
        
        # 檢查是否有標題列 (第一列是否為空)
        headers = worksheet.row_values(1)
        if not headers:
            print(f">> 試算表是空的，正在建立標題列...")
            worksheet.append_row(['date', 'query', 'page', 'clicks', 'impressions', 'ctr', 'position'])
        
        return worksheet
    except Exception as e:
        print(f"錯誤：無法開啟試算表 '{SHEET_NAME}'。請確認已分享權限給機器人 Email。")
        print(f"詳細錯誤: {e}")
        return None

def get_last_date(worksheet):
    """ 從試算表取得最後一筆資料的日期 """
    try:
        # 取得所有日期欄位 (第一欄，排除標題)
        dates = worksheet.col_values(1)[1:]
        if not dates:
            return None
        return max(dates)
    except Exception as e:
        print(f">> 讀取最後日期失敗: {e}")
        return None

def fetch_gsc_data():
    print(f"[{datetime.datetime.now()}] 開始執行 GSC 資料抓取任務...")
    
    creds = get_credentials()
    if not creds:
        print("錯誤：找不到 GCP_CREDENTIALS 環境變數或 credentials.json")
        return
    
    worksheet = init_sheet()
    if not worksheet:
        return

    service = build('searchconsole', 'v1', credentials=creds)

    last_date_in_sheet = get_last_date(worksheet)
    today = datetime.date.today()
    end_date = today - datetime.timedelta(days=3)

    if last_date_in_sheet is None:
        start_date = today - datetime.timedelta(days=400)
        print(">> 首次執行，抓取歷史資料...")
    else:
        last_date_obj = datetime.datetime.strptime(last_date_in_sheet, '%Y-%m-%d').date()
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
            data_to_append = []
            for row in rows:
                data_to_append.append([
                    row['keys'][0],    # date
                    row['keys'][1],    # query
                    row['keys'][2],    # page
                    row['clicks'],
                    row['impressions'],
                    row['ctr'],
                    row['position']
                ])
            
            # 使用 gspread 的 append_rows 批量存入
            worksheet.append_rows(data_to_append)
            print(f">> 成功存入 {len(data_to_append)} 筆資料到 Google Sheets！")
        else:
            print(">> 無新資料。")
            
    except Exception as e:
        print(f">> GSC 資料抓取發生錯誤: {e}")

def fetch_ga4_data():
    """ GA4 資料抓取預留位置 """
    print(f"[{datetime.datetime.now()}] (預留) 開始執行 GA4 資料抓取任務...")
    pass

if __name__ == "__main__":
    fetch_gsc_data()
    fetch_ga4_data()
