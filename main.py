APP_REV = "2026-02-24_14"

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

# --- 구글 시트 클라이언트 (Cloud 전용) ---
def gspread_client() -> gspread.Client:
    raw = get_env("GOOGLE_SERVICE_ACCOUNT_JSON").strip()
    try:
        info = json.loads(raw) if raw.startswith("{") else json.loads(base64.b64decode(raw).decode("utf-8"))
    except:
        info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=scopes))

def open_target_worksheet(gc: gspread.Client):
    sh = gc.open_by_key(get_env("GOOGLE_SHEET_ID").strip())
    ws = sh.worksheet(get_env("SHEET_NAME", "SAT Raw").strip())
    return sh, ws

# --- 데이터 파싱 및 날짜 처리 ---
EXPECTED_HEADERS = ["일자-No.", "품목명(규격)", "수량", "단가", "공급가액", "부가세", "합계", "거래처명", "적요", "거래처계층그룹명"]

def detect_month_key_from_rows(rows: List[List[Any]]) -> str:
    for r in rows:
        if not r: continue
        m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", str(r[0]))
        if m: return f"{m.group(1)}/{m.group(2)}"
    return datetime.datetime.now().strftime("%Y/%m")

def ym_key_from_a(a_val: Any) -> str:
    m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", str(a_val or ""))
    return f"{m.group(1)}/{m.group(2)}" if m else ""

# --- 핵심 ERP 프로세스 (메모리 방식 + 애플스크립트 로직 이식) ---
def ecount_download_and_parse() -> Tuple[bool, Dict[str, Any]]:
    if not PLAYWRIGHT_IMPORT_OK:
        return False, {"error": "Playwright import failed"}

    result = {"steps": {}}
    try:
        from openpyxl import load_workbook
        with sync_playwright() as p:
            # 브라우저 기동 (Cloud Run 리소스 최적화)
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(accept_downloads=True, viewport={'width': 1280, 'height': 1024})
            page = context.new_page()
            page.set_default_timeout(60000)

            # 1. 로그인 단계
            page.goto(get_env("ECOUNT_LOGIN_URL", "https://login.ecount.com/Login/"), wait_until="load")
            page.locator("#com_code").fill(get_env("COM_CODE"))
            page.locator("#id").fill(get_env("USER_ID"))
            page.locator("#passwd").fill(get_env("USER_PW"))
            page.keyboard.press("Enter")
            page.wait_for_load_state("networkidle")
            result["step_login"] = "done"

            # 보조 함수: 애플스크립트의 clickExact 로직을 모든 프레임 대상으로 재현
            def smart_click(txt_list: list, wait_sec: int = 15):
                if isinstance(txt_list, str): txt_list = [txt_list]
                limit = datetime.datetime.now() + datetime.timedelta(seconds=wait_sec)
                while datetime.datetime.now() < limit:
                    for frame in page.frames:
                        for txt in txt_list:
                            try:
                                target = frame.get_by_text(txt, exact=True).first
                                if target.count() > 0 and target.is_visible():
                                    target.click(force=True)
                                    return f"CLICKED:{txt}"
                            except: continue
                    page.wait_for_timeout(1000)
                return f"NOT_FOUND:{txt_list}"

            # 2. 애플스크립트 시퀀스 (2초 간격 딜레이 반영)
            result["steps"]["판매현황"] = smart_click("판매현황")
            page.wait_for_timeout(2000)
            
            result["steps"]["SAT"] = smart_click("SAT")
            page.wait_for_timeout(2000)
            
            result["steps"]["금월(~오늘)"] = smart_click("금월(~오늘)")
            page.wait_for_timeout(2000)

            # 3. 엑셀 다운로드 (메모리 버퍼 IO 방식)
            try:
                with page.expect_download(timeout=60000) as dlinfo:
                    # Excel(화면) 혹은 엑셀(화면) 등 다양한 표기 대응
                    result["steps"]["ExcelClick"] = smart_click(["Excel(화면)", "엑셀(화면)", "Excel"])
                
                download = dlinfo.value
                file_buffer = io.BytesIO()
                # 로컬 저장 없이 스트림을 통해 메모리에 직접 쓰기
                with download.create_read_stream() as stream:
                    while True:
                        chunk = stream.read(1024 * 64)
                        if not chunk: break
                        file_buffer.write(chunk)
                file_buffer.seek(0)

                # 4. 메모리 내 엑셀 데이터 추출
                wb = load_workbook(file_buffer, data_only=True, read_only=True)
                ws = wb.active # 첫 번째 시트 사용
                rows = []
                # 3행부터 시작하여 하단 합계 라인 제외
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
                return False, {"error": f"Download Stage Error: {str(e)}", "partial": result}

    except Exception as e:
        return False, {"error": f"Browser/Loop Error: {str(e)}", "partial": result}

# --- 전체 통합 실행 (Stage All) ---
def stage_all():
    try:
        gc = gspread_client()
        sh, ws = open_target_worksheet(gc)
        
        # 1. ERP 데이터 메모리 내 추출
        ok, erp_res = ecount_download_and_parse()
        if not ok: return {"ok": False, "error": erp_res}

        month_key = erp_res["month_key"]
        new_rows = erp_res["rows"]

        # 2. 구글 시트 기존 데이터 읽기
        all_vals = ws.get_all_values()
        header = all_vals[0] if all_vals else EXPECTED_HEADERS
        body = all_vals[1:]
        
        # 3. 동일 월 데이터 갱신 (Overwrite) 및 클라우드 업데이트
        kept = [r for r in body if ym_key_from_a(r[0]) != month_key]
        ws.clear()
        ws.update("A1", [header] + kept + new_rows, value_input_option="USER_ENTERED")

        return {"ok": True, "stage": "all", "month": month_key, "count": len(new_rows), "timestamp": now_kst_str()}
    except Exception as e:
        return {"ok": False, "error": str(e), "timestamp": now_kst_str()}

# --- 라우팅 ---
@app.route("/")
def health(): return f"OK | {APP_REV}", 200

@app.route("/run")
def run_job():
    # 모든 프로세스 통합 실행
    res = stage_all()
    return jsonify(res), (200 if res["ok"] else 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), threaded=True)
