APP_REV = "2026-02-24_15_full_debug"

from flask import Flask, jsonify
import os, json, base64, re, datetime, io, threading
from typing import Dict, Any, Tuple, List, Optional

import gspread
from google.oauth2.service_account import Credentials

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_IMPORT_OK = True
except Exception:
    PLAYWRIGHT_IMPORT_OK = False

app = Flask(__name__)

# -----------------------------------------------------------------------------
# 공통 유틸
# -----------------------------------------------------------------------------
def now_kst_str() -> str:
    kst = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S%z")

def get_env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    return v if v is not None else default

def env_bool(name: str, default: bool = False) -> bool:
    v = (get_env(name, "1" if default else "0") or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")

# -----------------------------------------------------------------------------
# 구글 시트 클라이언트
# -----------------------------------------------------------------------------
def gspread_client() -> gspread.Client:
    raw = get_env("GOOGLE_SERVICE_ACCOUNT_JSON").strip()
    try:
        info = json.loads(raw) if raw.startswith("{") else json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=scopes))

def open_target_worksheet(gc: gspread.Client):
    sh = gc.open_by_key(get_env("GOOGLE_SHEET_ID").strip())
    ws = sh.worksheet(get_env("SHEET_NAME", "SAT Raw").strip())
    return sh, ws

# -----------------------------------------------------------------------------
# 날짜 처리
# -----------------------------------------------------------------------------
def ym_key_from_a(a_val: Any) -> str:
    m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", str(a_val or ""))
    return f"{m.group(1)}/{m.group(2)}" if m else ""

# -----------------------------------------------------------------------------
# 디버그 수집(원인 분석용)
# -----------------------------------------------------------------------------
DEBUG_ON = env_bool("PW_DEBUG", True)  # 기본 True 권장

def _safe_eval(page, js: str):
    try:
        return page.evaluate(js)
    except Exception:
        return None

def capture_screenshot_b64(page) -> str:
    try:
        png = page.screenshot(full_page=True)
        return base64.b64encode(png).decode("utf-8")
    except Exception:
        return ""

def dump_frames_state(page, selectors: Dict[str, str]) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "page_url": page.url,
        "page_hash": _safe_eval(page, "location.hash"),
        "frames": []
    }
    for i, f in enumerate(page.frames):
        item: Dict[str, Any] = {
            "i": i,
            "name": getattr(f, "name", ""),
            "url": (getattr(f, "url", "") or "")[:220],
            "readyState": None,
            "selector_counts": {}
        }
        try:
            item["readyState"] = f.evaluate("document.readyState")
        except Exception:
            item["readyState"] = "?"

        for key, sel in selectors.items():
            try:
                item["selector_counts"][key] = f.locator(sel).count()
            except Exception:
                item["selector_counts"][key] = None

        data["frames"].append(item)
    return data

def attach_debug(result: Dict[str, Any], page, stage: str, extra: Optional[Dict[str, Any]] = None):
    if not DEBUG_ON:
        return

    selectors = {
        "inv": 'a#link_depth1_MENUTREE_000004',  # 재고 I
        "sales_mgmt": 'a[href*="prgId=C000030"][href*="menuSeq=MENUTREE_000030"]:has-text("영업관리")',
        "sales_status": 'a#link_depth4_MENUTREE_000494',  # 판매현황
        "sat": 'a[data-own-layer-box-id="layer_5_50863"]',
        "range_span": 'xpath=//span[normalize-space()="금월(~오늘)"]',
        "excel_btn": 'xpath=//span[normalize-space()="Excel(화면)"]/ancestor::a[1]',
    }

    result.setdefault("debug", {})
    pack = {
        "timestamp": now_kst_str(),
        "stage": stage,
        "frames_state": dump_frames_state(page, selectors=selectors),
        "screenshot_b64": capture_screenshot_b64(page),
    }
    if extra:
        pack["extra"] = extra
    result["debug"][stage] = pack

def html_snippet(locator, max_chars: int = 500) -> str:
    try:
        s = locator.evaluate("el => el.outerHTML")
        s = s.replace("\n", " ")
        return s[:max_chars]
    except Exception:
        return ""

# -----------------------------------------------------------------------------
# Playwright 클릭 유틸(실패 원인 추적 가능하게 설계)
# -----------------------------------------------------------------------------
def _iter_frames(page):
    return list(page.frames)

def click_first_visible_in_frames(page, css: str, timeout_ms: int, desc: str, result: Dict[str, Any]):
    deadline = datetime.datetime.now() + datetime.timedelta(milliseconds=timeout_ms)
    last_err = None

    while datetime.datetime.now() < deadline:
        for frame in _iter_frames(page):
            try:
                loc = frame.locator(css)
                if loc.count() <= 0:
                    continue
                target = loc.first
                target.wait_for(state="visible", timeout=1500)
                target.scroll_into_view_if_needed()
                target.click()
                return f"CLICKED:{desc}"
            except Exception as e:
                last_err = str(e)
                continue

        page.wait_for_timeout(250)

    # 실패 시 원인 분석 패키지 부착
    attach_debug(result, page, f"fail_click_{desc}", extra={"css": css, "last_err": last_err})
    raise RuntimeError(f"Timeout click: {desc} | last_err={last_err}")

def click_xpath_first_visible_in_frames(page, xpath: str, timeout_ms: int, desc: str, result: Dict[str, Any]):
    return click_first_visible_in_frames(page, f"xpath={xpath}", timeout_ms, desc, result)

def click_text_exact_in_frames(page, text: str, timeout_ms: int, desc: str, result: Dict[str, Any]):
    deadline = datetime.datetime.now() + datetime.timedelta(milliseconds=timeout_ms)
    last_err = None

    while datetime.datetime.now() < deadline:
        for frame in _iter_frames(page):
            try:
                loc = frame.get_by_text(text, exact=True)
                if loc.count() <= 0:
                    continue
                target = loc.first
                target.wait_for(state="visible", timeout=1500)
                target.scroll_into_view_if_needed()
                target.click()
                return f"CLICKED:{desc}"
            except Exception as e:
                last_err = str(e)
                continue
        page.wait_for_timeout(250)

    attach_debug(result, page, f"fail_click_{desc}", extra={"text": text, "last_err": last_err})
    raise RuntimeError(f"Timeout click text: {desc} | last_err={last_err}")

def expect_download_by_clicking_candidates(page, candidates, timeout_each_ms: int, result: Dict[str, Any], stage: str):
    """
    동일 HTML 후보가 2개(또는 그 이상)일 때:
    실제 download 이벤트가 발생하는 후보만 채택.
    """
    n = candidates.count()
    if n <= 0:
        attach_debug(result, page, f"fail_{stage}", extra={"reason": "no_candidates"})
        raise RuntimeError(f"{stage}: no candidates")

    # 후보 HTML 스니펫을 남김(원인 분석용)
    cand_snips = []
    for i in range(min(n, 5)):
        cand_snips.append({
            "i": i,
            "html": html_snippet(candidates.nth(i), 600)
        })

    last_err = None
    for i in range(n):
        btn = candidates.nth(i)
        try:
            btn.wait_for(state="visible", timeout=60000)
            btn.scroll_into_view_if_needed()
            with page.expect_download(timeout=timeout_each_ms) as dlinfo:
                btn.click()
            dl = dlinfo.value
            attach_debug(result, page, f"ok_{stage}", extra={"picked_index": i, "candidates": cand_snips})
            return dl
        except Exception as e:
            last_err = str(e)
            continue

    attach_debug(result, page, f"fail_{stage}", extra={"candidates": cand_snips, "last_err": last_err})
    raise RuntimeError(f"{stage}: download not triggered | last_err={last_err}")

# -----------------------------------------------------------------------------
# ECOUNT 다운로드 & 파싱 (클릭 순서 확정 + 실패 원인 수집)
# -----------------------------------------------------------------------------
def ecount_download_and_parse() -> Tuple[bool, Dict[str, Any]]:
    if not PLAYWRIGHT_IMPORT_OK:
        return False, {"error": "Playwright import failed"}

    result: Dict[str, Any] = {"steps": {}, "app_rev": APP_REV}

    try:
        from openpyxl import load_workbook

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"]
            )

            ua = (get_env("PW_USER_AGENT", "").strip() or None)
            context = browser.new_context(
                accept_downloads=True,
                viewport={"width": 1920, "height": 1080},
                user_agent=ua
            )

            page = context.new_page()
            page.set_default_timeout(60000)

            # 1) 로그인
            page.goto(get_env("ECOUNT_LOGIN_URL", "https://login.ecount.com/Login/"), wait_until="load")
            page.locator("#com_code").fill(get_env("COM_CODE"))
            page.locator("#id").fill(get_env("USER_ID"))
            page.locator("#passwd").fill(get_env("USER_PW"))
            page.keyboard.press("Enter")

            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(800)

            result["steps"]["login"] = "done"
            attach_debug(result, page, "after_login")

            # -----------------------------------------------------------------
            # 2) 메뉴 계층 클릭 (확정)
            # 재고 I -> 영업관리 -> 판매현황
            # -----------------------------------------------------------------
            # (0) 재고 I
            result["steps"]["재고 I"] = click_first_visible_in_frames(
                page,
                'a#link_depth1_MENUTREE_000004',
                timeout_ms=60000,
                desc="재고 I(a#link_depth1_MENUTREE_000004)",
                result=result
            )
            page.wait_for_timeout(350)
            attach_debug(result, page, "after_click_inventoryI")

            # (1) 영업관리 (2개 존재 문제: href 조건으로 고정)
            result["steps"]["영업관리"] = click_first_visible_in_frames(
                page,
                'a[href*="prgId=C000030"][href*="menuSeq=MENUTREE_000030"]:has-text("영업관리")',
                timeout_ms=60000,
                desc="영업관리(prgId=C000030, menuSeq=MENUTREE_000030)",
                result=result
            )
            page.wait_for_timeout(350)
            attach_debug(result, page, "after_click_sales_mgmt")

            # (2) 판매현황
            result["steps"]["판매현황"] = click_first_visible_in_frames(
                page,
                'a#link_depth4_MENUTREE_000494',
                timeout_ms=60000,
                desc="판매현황(a#link_depth4_MENUTREE_000494)",
                result=result
            )
            page.wait_for_timeout(600)
            attach_debug(result, page, "after_click_sales_status")

            # -----------------------------------------------------------------
            # 3) SAT
            # -----------------------------------------------------------------
            result["steps"]["SAT"] = click_first_visible_in_frames(
                page,
                'a[data-own-layer-box-id="layer_5_50863"]',
                timeout_ms=60000,
                desc='SAT(a[data-own-layer-box-id="layer_5_50863"])',
                result=result
            )
            page.wait_for_timeout(600)
            attach_debug(result, page, "after_click_sat")

            # -----------------------------------------------------------------
            # 4) 금월(~오늘) (1개)
            # span이지만 실제 클릭은 a인 경우가 많아 a 우선
            # -----------------------------------------------------------------
            try:
                result["steps"]["금월(~오늘)"] = click_xpath_first_visible_in_frames(
                    page,
                    '//span[normalize-space()="금월(~오늘)"]/ancestor::a[1]',
                    timeout_ms=30000,
                    desc='금월(~오늘)(span->ancestor a)',
                    result=result
                )
            except Exception:
                result["steps"]["금월(~오늘)"] = click_text_exact_in_frames(
                    page,
                    "금월(~오늘)",
                    timeout_ms=30000,
                    desc='금월(~오늘)(text exact)',
                    result=result
                )

            page.wait_for_timeout(900)
            attach_debug(result, page, "after_click_range")

            # -----------------------------------------------------------------
            # 5) Excel(화면) 다운로드 (2개/HTML 동일 가능)
            # 후보를 프레임 전체에서 찾고, download 이벤트 발생하는 후보만 채택
            # -----------------------------------------------------------------
            download = None
            last_err = None

            for frame in _iter_frames(page):
                try:
                    candidates = frame.locator('xpath=//span[normalize-space()="Excel(화면)"]/ancestor::a[1]')
                    if candidates.count() <= 0:
                        continue
                    download = expect_download_by_clicking_candidates(
                        page,
                        candidates,
                        timeout_each_ms=25000,
                        result=result,
                        stage="excel_download"
                    )
                    result["steps"]["Excel(화면)"] = f"DOWNLOAD_TRIGGERED(frame_url={frame.url[:140]})"
                    break
                except Exception as e:
                    last_err = str(e)
                    continue

            if download is None:
                attach_debug(result, page, "fail_excel_not_found", extra={"last_err": last_err})
                raise RuntimeError(f"Excel(화면) 다운로드 트리거 실패: {last_err}")

            # -----------------------------------------------------------------
            # 6) 다운로드 파일을 메모리로 읽기
            # -----------------------------------------------------------------
            file_buffer = io.BytesIO()
            with download.create_read_stream() as stream:
                while True:
                    chunk = stream.read(1024 * 128)
                    if not chunk:
                        break
                    file_buffer.write(chunk)
            file_buffer.seek(0)

            # -----------------------------------------------------------------
            # 7) 엑셀 파싱
            # -----------------------------------------------------------------
            wb = load_workbook(file_buffer, data_only=True, read_only=True)
            ws = wb.active

            rows: List[List[Any]] = []
            # 기존과 동일: 3행~(max_row-2)
            for r in range(3, ws.max_row - 2):
                row_val = [ws.cell(row=r, column=c).value for c in range(1, 11)]
                if row_val[0]:
                    rows.append(row_val)

            result["row_count"] = len(rows)
            result["month_key"] = datetime.datetime.now().strftime("%Y/%m")
            if rows:
                m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", str(rows[0][0]))
                if m:
                    result["month_key"] = f"{m.group(1)}/{m.group(2)}"

            result["rows"] = rows

            browser.close()
            return True, result

    except Exception as e:
        # 최종 예외도 기록
        result["error"] = str(e)
        return False, result

# -----------------------------------------------------------------------------
# 통합 실행
# -----------------------------------------------------------------------------
def stage_all():
    try:
        gc = gspread_client()
        sh, ws = open_target_worksheet(gc)

        ok, erp_res = ecount_download_and_parse()
        if not ok:
            # 실패 시에도 debug/result 전체를 반환하여 원인 분석 가능
            return {"ok": False, "error": erp_res}

        month_key = erp_res["month_key"]
        new_rows = erp_res["rows"]

        all_vals = ws.get_all_values()
        header = all_vals[0] if all_vals else [
            "일자-No.", "품목명(규격)", "수량", "단가", "공급가액", "부가세", "합계", "거래처명", "적요", "거래처계층그룹명"
        ]
        body = all_vals[1:]

        kept = [r for r in body if ym_key_from_a(r[0]) != month_key]

        ws.clear()
        ws.update("A1", [header] + kept + new_rows, value_input_option="USER_ENTERED")

        return {"ok": True, "month": month_key, "count": len(new_rows), "timestamp": now_kst_str()}

    except Exception as e:
        return {"ok": False, "error": str(e)}

# -----------------------------------------------------------------------------
# 동시 실행 방지(선택)
# Cloud Run에서 /run이 겹치면 Playwright 2개가 떠서 timeout 확률이 급증합니다.
# -----------------------------------------------------------------------------
RUN_LOCK = threading.Lock()

@app.route("/")
def health():
    return f"OK | {APP_REV}", 200

@app.route("/run")
def run_job():
    # 동시 실행 방지: 필요 없으면 아래 5줄을 제거하셔도 됩니다.
    if not RUN_LOCK.acquire(blocking=False):
        return jsonify({"ok": False, "error": "Another run is in progress"}), 429
    try:
        res = stage_all()
        return jsonify(res), (200 if res.get("ok") else 500)
    finally:
        RUN_LOCK.release()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(get_env("PORT", "8080")), threaded=True)
