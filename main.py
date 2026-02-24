# app.py
# -----------------------------------------------------------------------------
# ✅ Ecount ERP Automation (Cloud Run / Playwright)
# - 핵심 수정: "리다이렉트(wait_for_url)"가 아니라
#   (1) 현재 URL에 loginca 도메인 포함 여부 + (2) ERP 메뉴/요소 등장 여부로 로그인 성공 판정
# - /run?stage=erp 로 호출
# -----------------------------------------------------------------------------

import os
import json
import base64
import traceback
from datetime import datetime, timezone, timedelta

from flask import Flask, jsonify, request

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

APP_REV = os.environ.get("APP_REV", "2026-02-24_erp_login_fix_v1")

# =========================
# Flask
# =========================
app = Flask(__name__)

# =========================
# Helpers
# =========================
def now_kst_str() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(tz=kst).strftime("%Y-%m-%d %H:%M:%S%z")

def safe_b64_png(page) -> str:
    try:
        png = page.screenshot(full_page=True)
        return base64.b64encode(png).decode("utf-8")
    except Exception:
        return ""

def tail_console(page, max_items=80):
    items = getattr(page, "_console_tail", [])
    return items[-max_items:]

def bind_console_tail(page):
    page._console_tail = []
    def _on_console(msg):
        try:
            page._console_tail.append({"type": msg.type, "text": msg.text})
        except Exception:
            pass
    page.on("console", _on_console)

def dump_cookies(context, max_items=12):
    try:
        cookies = context.cookies()
        sample = cookies[:max_items]
        for c in sample:
            if "value" in c and isinstance(c["value"], str) and len(c["value"]) > 120:
                c["value"] = c["value"][:120] + "...(truncated)"
        return {"cookie_count": len(cookies), "cookies_sample": sample}
    except Exception:
        return {"cookie_count": 0, "cookies_sample": []}

def attach_debug(result, page, stage, extra=None, context=None):
    result.setdefault("debug", {})
    payload = {
        "stage": stage,
        "timestamp": now_kst_str(),
        "page_url": getattr(page, "url", ""),
        "screenshot_b64": safe_b64_png(page),
        "console_tail": tail_console(page),
    }
    if context is not None:
        payload.update(dump_cookies(context))
    if extra:
        payload.update(extra)
    result["debug"][stage] = payload

def collect_login_error_text(page) -> str:
    # 로그인 실패/경고 문구가 화면에 있으면 잡아냄 (없으면 빈 문자열)
    candidates = [
        "div.msg",
        "div.error",
        "p.error",
        "span.error",
        "div.alert",
        "div#msg",
        "div#message",
    ]
    texts = []
    for sel in candidates:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                for i in range(min(loc.count(), 5)):
                    t = (loc.nth(i).inner_text() or "").strip()
                    if t:
                        texts.append(t)
        except Exception:
            pass
    # 중복 제거
    uniq = []
    for t in texts:
        if t not in uniq:
            uniq.append(t)
    return " | ".join(uniq)

def frame_locator_counts(page, selector_map):
    frames_info = []
    for i, fr in enumerate(page.frames):
        info = {"i": i, "name": fr.name, "url": fr.url, "readyState": ""}
        try:
            info["readyState"] = fr.evaluate("document.readyState")
        except Exception:
            info["readyState"] = "unknown"
        counts = {}
        for k, sel in selector_map.items():
            try:
                counts[k] = fr.locator(sel).count()
            except Exception:
                counts[k] = 0
        info["selector_counts"] = counts
        frames_info.append(info)
    return frames_info

# =========================
# Ecount selectors (예시)
# =========================
SELECTOR_MAP = {
    # login page
    "login_com": 'input[name="com_code"], input#com_code, input[name="company"], input#company',
    "login_id": 'input[name="user_id"], input#user_id, input[name="id"], input#id',
    "login_pw": 'input[name="user_pw"], input#user_pw, input[type="password"]',
    # ERP menu (재고 I) - 사용자 로그에 있던 inv count=1 기준
    "inv": 'a#link_depth1_MENUTREE_000004',
    # (필요시) 판매관리/매출현황 등
    "sales_mgmt": 'a#link_depth1_MENUTREE_000001, a#link_depth1_MENUTREE_000002',
    "sales_status": 'a:has-text("매출현황"), a:has-text("Sales")',
    # 엑셀 버튼/기간 등 (환경에 따라 추가)
    "excel_btn": 'button:has-text("엑셀"), a:has-text("엑셀"), button:has-text("Excel"), a:has-text("Excel")',
    "range_span": 'span:has-text("기간"), span:has-text("Range")',
}

# =========================
# 핵심: ERP 준비 완료 판정
# =========================
def wait_until_erp_ready(page, timeout_ms=60000, selector_map=None):
    """
    ✅ wait_for_url 대신:
    1) page.url에 loginca.ecount.com 포함 여부
    2) ERP 좌측 메뉴(예: 재고 I)가 어떤 frame에서든 등장하는지
    """
    import time
    selector_map = selector_map or {}
    deadline = time.time() + (timeout_ms / 1000)

    last = {"url": page.url, "reason": "init"}

    while time.time() < deadline:
        url = page.url or ""
        last["url"] = url

        if "loginca.ecount.com" in url or "ecount.com/ec5/view/erp" in url:
            # frames 스캔
            try:
                frames = frame_locator_counts(page, selector_map)
                last["frames"] = frames

                # inv(재고 I) 또는 ERP를 의미하는 메뉴가 잡히면 OK
                for fr in page.frames:
                    try:
                        if selector_map.get("inv") and fr.locator(selector_map["inv"]).count() > 0:
                            return True, {"url": url, "reason": "erp_menu_found", "frames_state": frames}
                    except Exception:
                        pass
            except Exception:
                pass

            page.wait_for_timeout(300)
            continue

        # 아직 login 도메인/중간 페이지라면 잠깐 대기
        page.wait_for_timeout(300)
        last["reason"] = "waiting_domain"

    last["reason"] = "timeout_wait_erp_ready"
    return False, last

# =========================
# ERP Stage
# =========================
def run_stage_erp():
    """
    환경변수:
      - ECOUNT_COM_CODE
      - ECOUNT_USER_ID
      - ECOUNT_USER_PW
    """
    com_code = os.environ.get("ECOUNT_COM_CODE", "").strip()
    user_id = os.environ.get("ECOUNT_USER_ID", "").strip()
    user_pw = os.environ.get("ECOUNT_USER_PW", "").strip()

    if not (com_code and user_id and user_pw):
        return False, {
            "error": "Missing env vars",
            "need": ["ECOUNT_COM_CODE", "ECOUNT_USER_ID", "ECOUNT_USER_PW"],
        }

    result = {
        "app_rev": APP_REV,
        "timestamp": now_kst_str(),
        "steps": {},
        "debug": {},
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        context = browser.new_context(
            locale="ko-KR",
            viewport={"width": 1920, "height": 1080},
        )
        page = context.new_page()
        bind_console_tail(page)

        try:
            # 1) 로그인 페이지
            login_url = "https://login.ecount.com/"
            page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
            attach_debug(result, page, "login_page_loaded", context=context, extra={
                "frames_state": {
                    "page_url": page.url,
                    "frames": frame_locator_counts(page, SELECTOR_MAP),
                }
            })

            # 2) 로그인 입력
            # (회사코드/아이디/비번 셀렉터는 계정/국가에 따라 다를 수 있어 fallback 처리)
            def fill_first(selector, value):
                loc = page.locator(selector)
                if loc.count() > 0:
                    loc.first.fill(value)
                    return True
                return False

            # 회사코드
            ok_com = fill_first(SELECTOR_MAP["login_com"], com_code)
            ok_id = fill_first(SELECTOR_MAP["login_id"], user_id)
            ok_pw = fill_first(SELECTOR_MAP["login_pw"], user_pw)

            if not (ok_com and ok_id and ok_pw):
                attach_debug(result, page, "login_fields_not_found", context=context, extra={
                    "ok_com": ok_com, "ok_id": ok_id, "ok_pw": ok_pw,
                    "frames_state": {
                        "page_url": page.url,
                        "frames": frame_locator_counts(page, SELECTOR_MAP),
                    }
                })
                browser.close()
                return False, {"error": "Login fields not found", "partial": result}

            # 3) 로그인 버튼 클릭
            # 에카운트 로그인 버튼은 여러 형태가 있으므로 넓게 잡음
            login_btn = page.locator(
                'button:has-text("로그인"), input[type="submit"], button[type="submit"], a:has-text("로그인")'
            )
            if login_btn.count() == 0:
                attach_debug(result, page, "login_button_not_found", context=context)
                browser.close()
                return False, {"error": "Login button not found", "partial": result}

            # 클릭 후 로딩 대기 (네비게이션 이벤트에 의존하지 않음)
            login_btn.first.click()
            page.wait_for_timeout(1200)

            # 4) ✅ 핵심: ERP 준비 완료 판정 (URL + ERP 메뉴 등장)
            ok, info = wait_until_erp_ready(page, timeout_ms=60000, selector_map=SELECTOR_MAP)
            result["steps"]["login_check"] = info

            if not ok:
                err_txt = collect_login_error_text(page)
                attach_debug(result, page, "login_not_redirected", context=context, extra={
                    "error_text": err_txt,
                    "frames_state": {
                        "page_url": page.url,
                        "frames": frame_locator_counts(page, SELECTOR_MAP),
                    },
                })
                browser.close()
                return False, {
                    "error": "ERP not ready after login (menu not found)",
                    "partial": result
                }

            # 5) ERP 도착 디버그
            attach_debug(result, page, "erp_loaded", context=context, extra={
                "frames_state": {
                    "page_url": page.url,
                    "frames": frame_locator_counts(page, SELECTOR_MAP),
                }
            })
            result["steps"]["login"] = "done"
            result["erp"] = {"url": page.url}

            browser.close()
            return True, result

        except PWTimeoutError as e:
            attach_debug(result, page, "timeout", context=context, extra={"err": str(e)})
            browser.close()
            return False, {"error": "timeout", "partial": result}

        except Exception as e:
            attach_debug(result, page, "exception", context=context, extra={
                "err": str(e),
                "trace": traceback.format_exc()[:5000],
            })
            browser.close()
            return False, {"error": "exception", "partial": result}

# =========================
# Routes
# =========================
@app.route("/", methods=["GET"])
def root():
    return "OK", 200

@app.route("/run", methods=["GET", "POST"])
def run_job():
    stage = (request.args.get("stage") or "").strip().lower()
    if stage in ("", "help"):
        return jsonify({
            "ok": True,
            "app_rev": APP_REV,
            "stages": ["env", "erp", "all"],
            "examples": [
                "/run?stage=env",
                "/run?stage=erp",
                "/run?stage=all",
            ],
            "timestamp": now_kst_str(),
        }), 200

    if stage == "env":
        return jsonify({
            "ok": True,
            "app_rev": APP_REV,
            "timestamp": now_kst_str(),
            "env": {
                "ECOUNT_COM_CODE": "✅" if os.environ.get("ECOUNT_COM_CODE") else "❌",
                "ECOUNT_USER_ID": "✅" if os.environ.get("ECOUNT_USER_ID") else "❌",
                "ECOUNT_USER_PW": "✅" if os.environ.get("ECOUNT_USER_PW") else "❌",
            }
        }), 200

    if stage == "erp":
        ok, payload = run_stage_erp()
        return jsonify({"ok": ok, **payload}), (200 if ok else 500)

    if stage == "all":
        ok_erp, erp_payload = run_stage_erp()
        ok = ok_erp
        return jsonify({
            "ok": ok,
            "app_rev": APP_REV,
            "timestamp": now_kst_str(),
            "erp": erp_payload.get("erp"),
            "error": erp_payload.get("error"),
            "partial": erp_payload.get("partial"),
        }), (200 if ok else 500)

    return jsonify({"ok": False, "error": f"Unknown stage: {stage}", "timestamp": now_kst_str()}), 400


# local run: python app.py
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
