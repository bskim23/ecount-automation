APP_REV = "2026-02-24_25"

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

EXCEL_SEL = "[data-item-key='excel_view_footer_toolbar']"

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


def read_xlsx_rows(path: str) -> Tuple[List[List[Any]], str]:
    """
    엑셀 파일에서 판매현황 시트의 데이터 행을 읽어 반환.
    ★ read_only=True + iter_rows(values_only=True) 로 랜덤 접근 없이 스트리밍 처리.
    반환: (rows, month_key)
    """
    from openpyxl import load_workbook

    wb = load_workbook(path, data_only=True, read_only=True)

    if "판매현황" not in wb.sheetnames:
        raise RuntimeError(f"sheet '판매현황' not found: {wb.sheetnames}")

    ws = wb["판매현황"]

    # 헤더 검증 (row 1 = 회사명 메타, row 2 = 컬럼헤더)
    # read_only iter_rows로 앞 2행만 확인
    meta_row = None
    header_row = None
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=2, min_col=1, max_col=10, values_only=True)):
        if i == 0:
            meta_row = row
        else:
            header_row = list(row)

    a1 = meta_row[0] if meta_row else None
    if not isinstance(a1, str) or "회사명" not in a1:
        raise RuntimeError(f"A1 meta pattern not found: {a1!r}")

    if header_row != EXPECTED_HEADERS:
        raise RuntimeError(f"header mismatch: {header_row}")

    # 데이터 행 전체 읽기 (row 3~끝)
    # iter_rows는 스트리밍이므로 한 번에 리스트로 수집
    all_rows = [
        list(r)
        for r in ws.iter_rows(min_row=3, min_col=1, max_col=10, values_only=True)
    ]

    # 뒤에서 빈 행 제거
    while all_rows and all_rows[-1][0] in (None, ""):
        all_rows.pop()

    # 마지막 3행은 합계/소계 행 → 제외
    rows = all_rows[:-3] if len(all_rows) > 3 else []

    if not rows:
        raise RuntimeError("no data rows after excluding last 3 summary rows")

    month_key = detect_month_key_from_rows(rows)

    wb.close()
    return rows, month_key


def ecount_download_and_validate() -> Tuple[bool, Dict[str, Any]]:
    if not PLAYWRIGHT_IMPORT_OK:
        return False, {"error": "Playwright import failed"}

    com_code  = get_env("COM_CODE").strip()
    user_id   = get_env("USER_ID").strip()
    user_pw   = get_env("USER_PW").strip()
    login_url = get_env("ECOUNT_LOGIN_URL", "https://login.ecount.com/Login/").strip()
    dl_dir    = get_env("DOWNLOAD_DIR", "/tmp").strip() or "/tmp"

    result: Dict[str, Any] = {
        "login_url": login_url,
        "download_dir": dl_dir,
    }

    try:
        print("[ERP] launching playwright...", flush=True)
        with sync_playwright() as p:
            print("[ERP] playwright started, launching chromium...", flush=True)
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            print("[ERP] chromium launched", flush=True)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            page.set_default_timeout(30000)
            page.set_default_navigation_timeout(30000)

            # 1) 로그인
            print(f"[ERP] goto {login_url}", flush=True)
            page.goto(login_url, wait_until="commit", timeout=30000)
            page.wait_for_timeout(3000)

            try:
                page.locator("#com_code").fill(com_code)
                page.locator("#id").fill(user_id)
                page.locator("#passwd").fill(user_pw)
            except Exception as e:
                result["fill_error"] = repr(e)

            page.keyboard.press("Enter")
            print("[ERP] login Enter, waiting 5s...", flush=True)
            page.wait_for_timeout(5000)
            result["step_login"] = "done"
            result["url_after_login"] = page.url
            print(f"[ERP] login done url={page.url}", flush=True)

            # 2) 메뉴 클릭
            def click_text(txt: str) -> bool:
                for ctx in [page] + page.frames:
                    try:
                        loc = ctx.locator(f"text={txt}")
                        if loc.count() > 0:
                            loc.first.click(timeout=5000, force=True)
                            return True
                        loc2 = ctx.locator(f"span:has-text('{txt}')")
                        if loc2.count() > 0:
                            loc2.first.click(timeout=5000, force=True)
                            return True
                    except Exception:
                        continue
                return False

            def click_menu(link_id: str, txt: str) -> bool:
                for ctx in [page] + page.frames:
                    try:
                        loc = ctx.locator(f"#{link_id}")
                        if loc.count() > 0:
                            loc.first.click(timeout=5000, force=True)
                            return True
                    except Exception:
                        continue
                return click_text(txt)

            ok_steps = {}
            result["frame_count"] = len(page.frames)

            print("[ERP] clicking 재고I...", flush=True)
            ok_steps["재고I"] = click_menu("link_depth1_MENUTREE_000004", "재고 I")
            result["steps"] = ok_steps
            page.wait_for_timeout(2000)

            print("[ERP] clicking 판매현황...", flush=True)
            ok_steps["판매현황"] = click_menu("link_depth4_MENUTREE_000494", "판매현황")
            page.wait_for_timeout(2000)

            print("[ERP] clicking SAT...", flush=True)
            ok_steps["SAT"] = click_text("SAT")
            page.wait_for_timeout(1500)

            print("[ERP] clicking 금월(~오늘)...", flush=True)
            ok_steps["금월(~오늘)"] = click_text("금월(~오늘)")

            # Excel 버튼이 DOM에 나타날 때까지 최대 15초 폴링
            print("[ERP] polling for Excel button (max 15s)...", flush=True)
            excel_ctx = None
            for i in range(30):  # 0.5s × 30 = 15s
                page.wait_for_timeout(500)
                for ctx in [page] + page.frames:
                    try:
                        if ctx.locator(EXCEL_SEL).count() > 0:
                            excel_ctx = ctx
                            break
                    except Exception:
                        continue
                if excel_ctx is not None:
                    result["excel_found_after_ms"] = (i + 1) * 500
                    break

            result["excel_wait_found"] = excel_ctx is not None
            print(f"[ERP] excel_wait_found={excel_ctx is not None}", flush=True)
            result["debug_frame_urls"] = [f.url for f in page.frames]

            # 3) Excel(화면) 클릭 + 다운로드
            excel_clicked = False
            download = None

            if excel_ctx is not None:
                try:
                    print("[ERP] clicking Excel button...", flush=True)
                    with page.expect_download(timeout=30000) as dlinfo:
                        excel_ctx.locator(EXCEL_SEL).first.click(timeout=5000, force=True)
                    print("[ERP] download started, getting value...", flush=True)
                    download = dlinfo.value
                    print(f"[ERP] download value obtained: {download.suggested_filename}", flush=True)
                    excel_clicked = True
                except Exception as e:
                    result["excel_click_error"] = repr(e)
                    print(f"[ERP] excel click/download error: {repr(e)}", flush=True)
            else:
                print("[ERP] excel_ctx is None, skipping click", flush=True)

            ok_steps["ExcelClick"] = excel_clicked
            print(f"[ERP] ExcelClick={excel_clicked}", flush=True)

            if not excel_clicked:
                browser.close()
                raise RuntimeError(
                    f"Excel button {'found but click/download failed' if excel_ctx else 'not found in DOM (JS not rendered?)'}"
                )

            save_path = os.path.join(dl_dir, download.suggested_filename)
            print(f"[ERP] saving to {save_path}...", flush=True)
            download.save_as(save_path)
            print("[ERP] save_as done", flush=True)
            result["downloaded_file"] = save_path

            browser.close()

        # 4) 엑셀 검증 ── 브라우저 닫은 뒤 처리 (iter_rows 스트리밍)
        print("[ERP] loading workbook (iter_rows)...", flush=True)
        rows, month_key = read_xlsx_rows(save_path)
        print(f"[ERP] workbook parsed: {len(rows)} rows, month={month_key}", flush=True)

        result["row_count"] = len(rows)
        result["month_key"] = month_key

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

    month_key       = erp_payload.get("month_key", "")
    downloaded_file = erp_payload.get("downloaded_file", "")

    # ★ read_xlsx_rows 재사용 (이미 validate에서 검증 완료)
    print("[ALL] re-reading xlsx rows for sheet update...", flush=True)
    rows, _ = read_xlsx_rows(downloaded_file)
    inserted = len(rows)

    values = ws.get_all_values()
    if not values:
        values = []
    header = values[0] if values else []
    body   = values[1:] if len(values) >= 2 else []

    kept    = []
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
