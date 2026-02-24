APP_REV = "2026-02-24_12"

from flask import Flask, request, jsonify
import os, json, base64, re, datetime, io
from typing import Dict, Any, Tuple, List

import gspread
from google.oauth2.service_account import Credentials

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
    PLAYWRIGHT_IMPORT_OK = True
except Exception:
    PLAYWRIGHT_IMPORT_OK = False

app = Flask(__name__)

# --- 공통 유틸리티 ---
def now_kst_str() -> str:
    kst = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S%z")

def get_env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    return v if v is not None else default

# --- 구글 시트 클라이언트 ---
def gspread_client() -> gspread.Client:
    raw = get_env("GOOGLE_SERVICE_ACCOUNT_JSON").strip()
    try:
        if raw.startswith("{"):
            info = json.loads(raw)
        else:
            info = json.loads(base64.b64decode(raw).decode("utf-8"))
    except:
        info = json.loads(raw)
        
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_target_worksheet(gc: gspread.Client):
    sh = gc.open_by_key(get_env("GOOGLE_SHEET_ID").strip())
    ws = sh.worksheet(get_env("SHEET_NAME", "SAT Raw").strip())
    return sh, ws

# --- 데이터 파싱 유틸리티 ---
EXPECTED_HEADERS = ["일자-No.", "품목명(규격)", "수량", "단가", "공급가액", "부가세", "합계", "거래처명", "적요", "거래처계층그룹명"]

def detect_month_key_from_rows(rows: List[List[Any]]) -> str:
    for r in rows:
        if not r or len(r) < 1: continue
        m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", str(r[0]))
        if m: return f"{m.group(1)}/{m.group(2)}"
    return datetime.datetime.now().strftime("%Y/%m")

def ym_key_from_a(a_val: Any) -> str:
    m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", str(a_val or ""))
    return f"{m.group(1)}/{m.group(2)}" if m else ""

# --- ERP 프로세스: 메모리 내 처리 (Event Loop 충돌 방지 로직 추가) ---
def ecount_download_and_parse() -> Tuple[bool, Dict[str, Any]]:
    if not PLAYWRIGHT_IMPORT_OK:
        return False, {"error": "Playwright failed"}

    result = {"steps": {}}
    
    # [핵심] sync_playwright()를 context manager로 확실히 닫아줌
    try:
        from openpyxl import load_workbook
        with sync_playwright() as p:
            # 브라우저 실행 옵션 최적화
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            page.set_default_timeout(60000) # 개별 타임아웃 60초

            # 1. 로그인
            page.goto(get_env("ECOUNT_LOGIN_URL", "https://login.ecount.com/Login/"), wait_until="load")
            page.locator("#com_code").fill(get_env("COM_CODE"))
            page.locator("#id").fill(get_env("USER_ID"))
            page.locator("#passwd").fill(get_env("USER_PW"))
            page.keyboard.press("Enter")
            
            # 로그인 후 안정화 대기
            page.wait_for_load_state("networkidle")
            result["step_login"] = "done"

            # 보조 함수: 프레임 순회 및 텍스트 클릭 (타임아웃 강화)
            def find_and_click(text: str, wait_sec: int = 15):
                limit = datetime.datetime.now() + datetime.timedelta(seconds=wait_sec)
                while datetime.datetime.now() < limit:
                    for frame in page.frames:
                        try:
                            # 텍스트가 정확히 일치하는 요소를 찾아 클릭
                            loc = frame.get_by_text(text, exact=True).first
                            if loc.is_visible():
                                loc.click()
                                return True
                        except: continue
                    page.wait_for_timeout(1000)
                return False

            # 2. SAT -> 금월(~오늘) 클릭 시나리오
            result["steps"]["판매현황"] = find_and_click("판매현황")
            page.wait_for_timeout(2000)
            
            result["steps"]["SAT"] = find_and_click("SAT")
            page.wait_for_timeout(1000)
            
            result["steps"]["금월(~오늘)"] = find_and_click("금월(~오늘)")
            page.wait_for_timeout(2000)

            # 3. 엑셀 다운로드 및 메모리 스트림 처리
            try:
                with page.expect_download(timeout=60000) as dlinfo:
                    result["steps"]["ExcelClick"] = find_and_click("Excel(화면)")
                
                download = dlinfo.value
                file_buffer = io.BytesIO()
                # 스트림 방식으로 메모리에 직접 쓰기
                with download.create_read_stream() as stream:
                    while True:
                        chunk = stream.read(1024 * 64)
                        if not chunk: break
                        file_buffer.write(chunk)
                file_buffer.seek(0)

                # 4. 엑셀 데이터 파싱
                wb = load_workbook(file_buffer, data_only=True, read_only=True)
                ws = wb["판매현황"] if "판매현황" in wb.sheetnames else wb.active
                
                rows = []
                for r in range(3, ws.max_row - 2):
                    row_val = [ws.cell(row=r, column=c).value for c in range(1, 11)]
                    if row_val[0]: rows.append(row_val)

                result["row_count"] = len(rows)
                result["month_key"] = detect_month_key_from_rows(rows)
                result["rows"] = rows
                
                browser.close()
                return True, result
            except Exception as e:
                browser.close()
                return False, {"error": f"Download/Parse Error: {str(e)}", "partial": result}

    except Exception as e:
        return False, {"error": f"Event Loop/Browser Error: {str(e)}", "partial": result}

# --- 구글 시트 업데이트 (Cloud Sync) ---
def stage_all():
    try:
        gc = gspread_client()
        sh, ws = open_target_worksheet(gc)
        
        ok, erp_res = ecount_download_and_parse()
        if not ok: return {"ok": False, "error": erp_res}

        month_key = erp_res["month_key"]
        new_rows = erp_res["rows"]

        all_vals = ws.get_all_values()
        header = all_vals[0] if all_vals else EXPECTED_HEADERS
        body = all_vals[1:] if len(all_vals) > 1 else []

        kept = [r for r in body if ym_key_from_a(r[0]) != month_key]
        final_data = [header] + kept + new_rows

        ws.clear()
        ws.update("A1", final_data, value_input_option="USER_ENTERED")

        return {"stage": "all", "ok": True, "month": month_key, "inserted": len(new_rows), "timestamp": now_kst_str()}
    except Exception as e:
        return {"stage": "all", "ok": False, "error": str(e), "timestamp": now_kst_str()}

@app.route("/")
def health(): return f"OK | {APP_REV}", 200

@app.route("/run")
def run_job():
    res = stage_all()
    return jsonify(res), (200 if res["ok"] else 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), threaded=True)
