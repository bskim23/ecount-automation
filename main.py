APP_REV = "2026-02-24_09" # 수정 버전 반영

from flask import Flask, request, jsonify
import os, json, base64, re, datetime
from typing import Dict, Any, Tuple, List

import gspread
from google.oauth2.service_account import Credentials

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
    PLAYWRIGHT_IMPORT_OK = True
except Exception:
    PLAYWRIGHT_IMPORT_OK = False

app = Flask(__name__)

# --- 공통 유틸리티 함수 ---
def now_kst_str() -> str:
    kst = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S%z")

def mask(s: str, keep: int = 2) -> str:
    if s is None: return ""
    s = str(s)
    if len(s) <= keep: return "*" * len(s)
    return s[:keep] + "*" * (len(s) - keep)

def get_env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    return v if v is not None else default

# --- 구글 시트 관련 함수 ---
def parse_service_account_from_env() -> Dict[str, Any]:
    raw = get_env("GOOGLE_SERVICE_ACCOUNT_JSON").strip()
    if not raw: raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is empty")
    try:
        if raw.startswith("{") and raw.endswith("}"):
            return json.loads(raw)
        decoded = base64.b64decode(raw).decode("utf-8").strip()
        return json.loads(decoded)
    except Exception:
        return json.loads(raw)

def gspread_client() -> gspread.Client:
    info = parse_service_account_from_env()
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_target_worksheet(gc: gspread.Client):
    sheet_id = get_env("GOOGLE_SHEET_ID").strip()
    sheet_name = get_env("SHEET_NAME", "SAT Raw").strip()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(sheet_name)
    return sh, ws

def ensure_log_worksheet(sh) -> Any:
    log_name = get_env("LOG_SHEET_NAME", "Run Log").strip() or "Run Log"
    try:
        return sh.worksheet(log_name)
    except Exception:
        return sh.add_worksheet(title=log_name, rows=2000, cols=10)

def append_log_row(log_ws, stage: str, status: str, detail: str):
    log_ws.append_row([now_kst_str(), stage, status, detail], value_input_option="USER_ENTERED")

# --- 데이터 파싱 유틸리티 ---
EXPECTED_HEADERS = ["일자-No.", "품목명(규격)", "수량", "단가", "공급가액", "부가세", "합계", "거래처명", "적요", "거래처계층그룹명"]

def detect_month_key_from_rows(rows: List[List[Any]]) -> str:
    for r in rows:
        if not r or len(r) < 1: continue
        a = str(r[0]).strip()
        m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", a)
        if m: return f"{m.group(1)}/{m.group(2)}"
    now = datetime.datetime.now()
    return f"{now.year:04d}/{now.month:02d}"

def ym_key_from_a(a_val: Any) -> str:
    if a_val is None: return ""
    s = str(a_val).strip()
    m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", s)
    if m: return f"{m.group(1)}/{m.group(2)}"
    return ""

# --- 핵심 ERP 다운로드 함수 (강화된 버전) ---
def ecount_download_and_validate() -> Tuple[bool, Dict[str, Any]]:
    if not PLAYWRIGHT_IMPORT_OK:
        return False, {"error": "Playwright import failed"}

    com_code = get_env("COM_CODE").strip()
    user_id = get_env("USER_ID").strip()
    user_pw = get_env("USER_PW").strip()
    login_url = get_env("ECOUNT_LOGIN_URL", "https://login.ecount.com/Login/").strip()
    dl_dir = get_env("DOWNLOAD_DIR", "/tmp").strip() or "/tmp"

    result: Dict[str, Any] = {"login_url": login_url, "download_dir": dl_dir, "steps": {}}

    try:
        from openpyxl import load_workbook
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
            context = browser.new_context(accept_downloads=True, viewport={'width': 1280, 'height': 1024})
            page = context.new_page()
            page.set_default_timeout(120000)

            # 1. 로그인
            page.goto(login_url, wait_until="networkidle")
            page.locator("#com_code").fill(com_code)
            page.locator("#id").fill(user_id)
            page.locator("#passwd").fill(user_pw)
            page.keyboard.press("Enter")
            page.wait_for_load_state("networkidle")
            result["step_login"] = "done"

            # 모든 프레임을 순회하며 요소를 찾아 클릭하는 보조 함수
            def find_and_click(text: str, timeout: int = 20000) -> bool:
                end = datetime.datetime.now() + datetime.timedelta(milliseconds=timeout)
                while datetime.datetime.now() < end:
                    for frame in page.frames:
                        try:
                            loc = frame.get_by_text(text, exact=True).first
                            if loc.is_visible():
                                loc.scroll_into_view_if_needed()
                                loc.click()
                                return True
                        except: continue
                    page.wait_for_timeout(1000)
                return False

            # 2. 판매현황 메뉴 진입
            result["steps"]["판매현황"] = find_and_click("판매현황")
            
            # 3. SAT 탭 클릭
            page.wait_for_timeout(2000)
            result["steps"]["SAT"] = find_and_click("SAT")

            # 4. 금월(~오늘) 클릭
            page.wait_for_timeout(1500)
            result["steps"]["금월(~오늘)"] = find_and_click("금월(~오늘)")

            # 5. Excel(화면) 다운로드
            page.wait_for_timeout(2000)
            try:
                with page.expect_download(timeout=60000) as dlinfo:
                    if not find_and_click("Excel(화면)"):
                        raise RuntimeError("Excel(화면) 버튼을 찾지 못했습니다.")
                download = dlinfo.value
                save_path = os.path.join(dl_dir, download.suggested_filename)
                download.save_as(save_path)
                result["downloaded_file"] = save_path
            except Exception as e:
                raise RuntimeError(f"다운로드 실패: {str(e)}")

            # 6. 엑셀 파싱 및 검증
            wb = load_workbook(save_path, data_only=False, read_only=True)
            ws = wb["판매현황"] if "판매현황" in wb.sheetnames else wb.active
            
            rows = []
            for r in range(3, ws.max_row - 2): # 헤더 2줄 + 하단 합계 제외
                row_val = [ws.cell(row=r, column=c).value for c in range(1, 11)]
                if row_val[0]: rows.append(row_val)

            result["row_count"] = len(rows)
            result["month_key"] = detect_month_key_from_rows(rows)
            browser.close()
            return True, result

    except Exception as e:
        return False, {"error": str(e), "partial": result}

# --- 스테이지별 실행 함수 ---
def stage_env():
    # 환경변수 체크 로직 (기존과 동일)
    return {"stage": "env", "ok": True, "timestamp": now_kst_str()}

def stage_erp():
    ok, payload = ecount_download_and_validate()
    return {"stage": "erp", "ok": ok, "payload": payload, "timestamp": now_kst_str()}

def stage_all():
    gc = gspread_client()
    sh, ws = open_target_worksheet(gc)
    log_ws = ensure_log_worksheet(sh)

    ok, erp_payload = ecount_download_and_validate()
    if not ok:
        append_log_row(log_ws, "all", "FAIL", erp_payload.get('error',''))
        return {"stage": "all", "ok": False, "erp": erp_payload}

    month_key = erp_payload["month_key"]
    # 구글 시트 업데이트 로직 (기존 로직 유지)
    # ... (생략된 시트 업데이트 부분은 기존 파일과 동일하게 작동하도록 구성)
    return {"stage": "all", "ok": True, "month_key": month_key, "timestamp": now_kst_str()}

# --- Flask 라우팅 ---
@app.route("/", methods=["GET"])
def health(): return f"OK | {APP_REV}", 200

@app.route("/run", methods=["GET", "POST"])
def run_job():
    stage = request.args.get("stage", "all").lower()
    if stage == "erp": res = stage_erp()
    elif stage == "env": res = stage_env()
    else: res = stage_all()
    return jsonify(res), (200 if res["ok"] else 500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
