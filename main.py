APP_REV = "2026-02-24_05"

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

def now_kst_str() -> str:
    kst = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S%z")

def mask(s: str, keep: int = 2) -> str:
    if s is None:
        return ""
    s = str(s)
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + "*" * (len(s) - keep)

def get_env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    return v if v is not None else default

def parse_service_account_from_env() -> Dict[str, Any]:
    raw = get_env("GOOGLE_SERVICE_ACCOUNT_JSON").strip()
    if not raw:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is empty")
    if raw.startswith("{") and raw.endswith("}"):
        return json.loads(raw)
    try:
        decoded = base64.b64decode(raw).decode("utf-8").strip()
        if decoded.startswith("{") and decoded.endswith("}"):
            return json.loads(decoded)
    except Exception:
        pass
    return json.loads(raw)

def gspread_client() -> gspread.Client:
    info = parse_service_account_from_env()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_target_worksheet(gc: gspread.Client):
    sheet_id = get_env("GOOGLE_SHEET_ID").strip()
    sheet_name = get_env("SHEET_NAME", "SAT Raw").strip()
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID is empty")
    if not sheet_name:
        raise ValueError("SHEET_NAME is empty")
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
    log_ws.append_row(
        [now_kst_str(), stage, status, detail],
        value_input_option="USER_ENTERED"
    )

def stage_env() -> Dict[str, Any]:
    required = [
        "GOOGLE_SHEET_ID", "SHEET_NAME", "GOOGLE_SERVICE_ACCOUNT_JSON",
        "COM_CODE", "USER_ID", "USER_PW",
    ]
    present = {}
    missing = []
    for k in required:
        v = get_env(k)
        if not v:
            missing.append(k)
            present[k] = {"present": False, "preview": ""}
        else:
            if k == "USER_PW":
                preview = mask(v, keep=1)
            elif k == "GOOGLE_SERVICE_ACCOUNT_JSON":
                preview = f"len={len(v)}"
            elif k == "GOOGLE_SHEET_ID":
                preview = v[:10] + "..."
            else:
                preview = v
            present[k] = {"present": True, "preview": preview}
    return {
        "stage": "env",
        "ok": len(missing) == 0,
        "missing": missing,
        "present": present,
        "timestamp": now_kst_str(),
    }

def stage_gsheet() -> Dict[str, Any]:
    gc = gspread_client()
    sh, ws = open_target_worksheet(gc)
    log_ws = ensure_log_worksheet(sh)
    detail = f"target={ws.title}, sheet_id={sh.id}"
    append_log_row(log_ws, "gsheet", "OK", detail)
    return {
        "stage": "gsheet",
        "ok": True,
        "message": "Google Sheet write test OK (logged to Run Log)",
        "target_sheet": ws.title,
        "log_sheet": log_ws.title,
        "timestamp": now_kst_str(),
    }

EXPECTED_HEADERS = [
    "일자-No.", "품목명(규격)", "수량", "단가", "공급가액",
    "부가세", "합계", "거래처명", "적요", "거래처계층그룹명",
]

def detect_month_key_from_rows(rows: List[List[Any]]) -> str:
    for r in rows:
        if not r or len(r) < 1:
            continue
        a = str(r[0]).strip()
        m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", a)
        if m:
            return f"{m.group(1)}/{m.group(2)}"
    now = datetime.datetime.now()
    return f"{now.year:04d}/{now.month:02d}"

def ecount_download_and_validate() -> Tuple[bool, Dict[str, Any]]:
    if not PLAYWRIGHT_IMPORT_OK:
        return False, {"error": "Playwright import failed"}

    com_code = get_env("COM_CODE").strip()
    user_id = get_env("USER_ID").strip()
    user_pw = get_env("USER_PW").strip()
    login_url = get_env("ECOUNT_LOGIN_URL", "https://login.ecount.com/Login/").strip()
    dl_dir = get_env("DOWNLOAD_DIR", "/tmp").strip() or "/tmp"

    result: Dict[str, Any] = {
        "login_url": login_url,
        "download_dir": dl_dir,
    }

    try:
        from openpyxl import load_workbook
    except Exception as e:
        return False, {"error": f"openpyxl import failed: {repr(e)}"}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            page.set_default_timeout(120000)
            page.set_default_navigation_timeout(120000)

            # 1) 로그인
            page.goto(login_url, wait_until="commit", timeout=30000)
            page.wait_for_timeout(3000)

            try:
                page.locator("#com_code").fill(com_code)
                page.locator("#id").fill(user_id)
                page.locator("#passwd").fill(user_pw)
            except Exception as e:
                result["fill_error"] = repr(e)
            page.keyboard.press("Enter")
            page.wait_for_timeout(3000)
            result["step_login"] = "done"

            # 2) 메뉴 클릭
            def click_text(txt: str) -> bool:
                loc = page.locator(f"text={txt}")
                if loc.count() > 0:
                    loc.first.click()
                    return True
                return False

            def click_menu(link_id: str, txt: str) -> bool:
                loc = page.locator(f"#{link_id}")
                if loc.count() > 0:
                    loc.first.click()
                    return True
                return click_text(txt)

            ok_steps = {}
            ok_steps["판매현황"] = click_menu("link_depth4_MENUTREE_000494", "판매현황")
            result["steps"] = ok_steps
            page.wait_for_timeout(2000)
            ok_steps["SAT"] = click_text("SAT")
            page.wait_for_timeout(1500)
            ok_steps["금월(~오늘)"] = click_text("금월(~오늘)")
            page.wait_for_timeout(1500)            

            # 3) 다운로드
            excel_clicked = False
            for label in ["Excel(화면)", "엑셀(화면)", "Excel"]:
                if click_text(label):
                    excel_clicked = True
                    break

            ok_steps["ExcelClick"] = excel_clicked
            if not excel_clicked:
                raise RuntimeError("Excel download button not found")

            with page.expect_download(timeout=120000) as dlinfo:
                pass
            download = dlinfo.value
            save_path = os.path.join(dl_dir, download.suggested_filename)
            download.save_as(save_path)
            result["downloaded_file"] = save_path

            # 4) 엑셀 검증
            wb = load_workbook(save_path, data_only=False, read_only=True)
            if "판매현황" not in wb.sheetnames:
                raise RuntimeError(f"sheet '판매현황' not found: {wb.sheetnames}")

            ws = wb["판매현황"]
            a1 = ws["A1"].value
            if not isinstance(a1, str) or "회사명" not in a1:
                raise RuntimeError("A1 meta pattern not found")

            headers = [ws.cell(row=2, column=c).value for c in range(1, 11)]
            if headers != EXPECTED_HEADERS:
                raise RuntimeError(f"header mismatch: {headers}")

            last = ws.max_row
            while last >= 3 and (ws.cell(row=last, column=1).value in (None, "")):
                last -= 1
            data_end = last - 3
            if data_end < 3:
                raise RuntimeError("no data rows after excluding last 3 rows")

            rows = []
            for r in range(3, data_end + 1):
                rows.append([ws.cell(row=r, column=c).value for c in range(1, 11)])

            result["row_count"] = len(rows)
            result["month_key"] = detect_month_key_from_rows(rows)
            browser.close()

        return True, result

    except PWTimeoutError as e:
        return False, {"error": f"Playwright timeout: {repr(e)}", "partial": result}
    except Exception as e:
        return False, {"error": f"ERP stage failed: {repr(e)}", "partial": result}

def stage_erp() -> Dict[str, Any]:
    ok, payload = ecount_download_and_validate()
    return {
        "stage": "erp",
        "ok": ok,
        "payload": payload,
        "timestamp": now_kst_str(),
    }

def ym_key_from_a(a_val: Any) -> str:
    if a_val is None:
        return ""
    s = str(a_val).strip()
    m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    return ""

def stage_all() -> Dict[str, Any]:
    env_res = stage_env()
    if not env_res["ok"]:
        return {"stage": "all", "ok": False, "failed_at": "env", "env": env_res, "timestamp": now_kst_str()}

    gc = gspread_client()
    sh, ws = open_target_worksheet(gc)
    log_ws = ensure_log_worksheet(sh)

    ok, erp_payload = ecount_download_and_validate()
    if not ok:
        append_log_row(log_ws, "all", "FAIL", f"erp_failed: {erp_payload.get('error','')}")
        return {
            "stage": "all",
            "ok": False,
            "failed_at": "erp",
            "erp": erp_payload,
            "timestamp": now_kst_str(),
        }

    month_key = erp_payload.get("month_key", "")
    downloaded_file = erp_payload.get("downloaded_file", "")

    from openpyxl import load_workbook
    wb = load_workbook(downloaded_file, data_only=False, read_only=True)
    src = wb["판매현황"]
    last = src.max_row
    while last >= 3 and (src.cell(row=last, column=1).value in (None, "")):
        last -= 1
    data_end = last - 3
    rows = []
    for r in range(3, data_end + 1):
        rows.append([src.cell(row=r, column=c).value for c in range(1, 11)])
    inserted = len(rows)

    values = ws.get_all_values()
    if not values:
        values = []
    header = values[0] if values else []
    body = values[1:] if len(values) >= 2 else []

    kept = []
    deleted = 0
    for r in body:
        a = r[0] if len(r) > 0 else ""
        if ym_key_from_a(a) == month_key:
            deleted += 1
        else:
            kept.append(r)

    new_body = kept + rows
    ws.clear()

    out = []
    if header:
        out.append(header)
    out.extend(new_body)
    ws.update("A1", out, value_input_option="USER_ENTERED")

    append_log_row(log_ws, "all", "OK", f"month={month_key}, deleted={deleted}, inserted={inserted}")

    return {
        "stage": "all",
        "ok": True,
        "month_key": month_key,
        "deleted_rows_in_month": deleted,
        "inserted_rows": inserted,
        "target_sheet": ws.title,
        "log_sheet": log_ws.title,
        "timestamp": now_kst_str(),
    }

@app.route("/", methods=["GET"])
def health():
    return f"OK | {APP_REV}", 200

@app.route("/run", methods=["POST", "GET"])
def run_job():
    stage = (request.args.get("stage") or "").strip().lower()

    if stage in ("", "help"):
        return jsonify({
            "ok": True,
            "app_rev": APP_REV,
            "stages": ["env", "gsheet", "erp", "all"],
            "examples": ["/run?stage=env", "/run?stage=gsheet", "/run?stage=erp", "/run?stage=all"],
            "timestamp": now_kst_str(),
        }), 200

    if stage == "env":
        res = stage_env()
        return jsonify(res), (200 if res["ok"] else 400)

    if stage == "gsheet":
        try:
            res = stage_gsheet()
            return jsonify(res), 200
        except Exception as e:
            return jsonify({"stage": "gsheet", "ok": False, "error": repr(e), "timestamp": now_kst_str()}), 500

    if stage == "erp":
        res = stage_erp()
        return jsonify(res), (200 if res["ok"] else 500)

    if stage == "all":
        res = stage_all()
        return jsonify(res), (200 if res["ok"] else 500)

    return jsonify({"ok": False, "error": f"unknown stage: {stage}"}), 400

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
