# app.py
import os
import time
import traceback
from datetime import datetime

from flask import Flask, jsonify, request
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

APP_REV = "2026-02-24_erp_click_sequence_v4_excel_required"

app = Flask(__name__)


def now_kst_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def env_flag(name: str) -> str:
    return "✅" if os.environ.get(name) else "❌"


def snap_text(page, label: str):
    dbg = {"label": label, "ts": now_kst_str()}
    try:
        dbg["page_url"] = page.url
    except Exception as e:
        dbg["page_url_err"] = repr(e)

    targets = {
        "inv_i": "text=재고 I",
        "inv": "text=재고",
        "sales_status": "text=판매현황",
        "sat": "text=SAT",
        "this_month_today": "text=금월(~오늘)",
        "excel_screen": "text=Excel(화면)",
    }

    frames = []
    try:
        for i, fr in enumerate(page.frames):
            fr_info = {"i": i, "name": fr.name, "url": fr.url}
            try:
                fr_info["readyState"] = fr.evaluate("document.readyState")
            except Exception:
                fr_info["readyState"] = None

            counts = {}
            for k, sel in targets.items():
                try:
                    counts[k] = fr.locator(sel).count()
                except Exception as e:
                    counts[k] = f"err:{type(e).__name__}"
            fr_info["counts"] = counts
            frames.append(fr_info)

        dbg["frames"] = frames
    except Exception as e:
        dbg["frames_err"] = repr(e)

    return dbg


def _safe_click_text(ctx, text: str, timeout_ms: int = 5000) -> bool:
    loc = ctx.locator(f"text={text}").first
    if loc.count() > 0:
        loc.wait_for(state="visible", timeout=timeout_ms)
        loc.click(timeout=timeout_ms)
        return True
    return False


def click_text_anywhere(page, text: str, timeout_ms: int, dbg_steps: list):
    end = time.time() + timeout_ms / 1000
    last_err = None

    while time.time() < end:
        try:
            if _safe_click_text(page, text):
                dbg_steps.append({"type": "click_ok", "text": text, "snap": snap_text(page, f"after_click:{text}")})
                return True

            for fr in page.frames:
                try:
                    if _safe_click_text(fr, text):
                        dbg_steps.append({"type": "click_ok", "text": text, "snap": snap_text(page, f"after_click:{text}")})
                        return True
                except Exception as e:
                    last_err = e

        except Exception as e:
            last_err = e

        time.sleep(0.25)

    dbg_steps.append({
        "type": "click_fail",
        "text": text,
        "last_err": repr(last_err),
        "snap": snap_text(page, f"click_fail:{text}")
    })
    raise RuntimeError(f"[CLICK_FAIL] text='{text}' last_err={last_err}")


def wait_text_anywhere(page, text: str, timeout_ms: int, dbg_steps: list):
    end = time.time() + timeout_ms / 1000
    while time.time() < end:
        try:
            if page.locator(f"text={text}").count() > 0:
                return True
            for fr in page.frames:
                try:
                    if fr.locator(f"text={text}").count() > 0:
                        return True
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(0.25)

    dbg_steps.append({"type": "wait_text_fail", "text": text, "snap": snap_text(page, f"wait_fail:{text}")})
    raise RuntimeError(f"[WAIT_FAIL] text='{text}'")


ECOUNT_LOGIN_URL = "https://login.ecount.com/"


def require_env():
    need = ["ECOUNT_COM_CODE", "ECOUNT_USER_ID", "ECOUNT_USER_PW"]
    missing = [k for k in need if not os.environ.get(k)]
    return missing


def do_login(page, dbg_steps: list):
    com = os.environ["ECOUNT_COM_CODE"]
    uid = os.environ["ECOUNT_USER_ID"]
    pw = os.environ["ECOUNT_USER_PW"]

    page.goto(ECOUNT_LOGIN_URL, wait_until="domcontentloaded")
    dbg_steps.append({"type": "snap", "snap": snap_text(page, "login_page_loaded")})

    selectors = {
        "com": [
            'input[name="com_code"]',
            'input[name="CompanyNo"]',
            'input[name="comCode"]',
            'input[id*="com"]',
            'input[placeholder*="회사"]',
        ],
        "id": [
            'input[name="user_id"]',
            'input[name="UserId"]',
            'input[name="id"]',
            'input[id*="id"]',
            'input[placeholder*="아이디"]',
        ],
        "pw": [
            'input[type="password"]',
            'input[name="user_pw"]',
            'input[name="UserPW"]',
            'input[id*="pw"]',
            'input[placeholder*="비밀번호"]',
        ],
        "submit": [
            'button:has-text("로그인")',
            'input[type="submit"]',
            'button[type="submit"]',
            'button:has-text("Login")',
        ]
    }

    def fill_first(sel_list, value):
        last = None
        for sel in sel_list:
            try:
                loc = page.locator(sel).first
                if loc.count() > 0:
                    loc.click(timeout=3000)
                    loc.fill(value, timeout=3000)
                    return True
            except Exception as e:
                last = e
        return False

    ok_com = fill_first(selectors["com"], com)
    ok_id = fill_first(selectors["id"], uid)
    ok_pw = fill_first(selectors["pw"], pw)

    if not (ok_com and ok_id and ok_pw):
        dbg_steps.append({"type": "login_fill_fail", "snap": snap_text(page, "login_fill_fail")})
        raise RuntimeError(f"[LOGIN_FILL_FAIL] com={ok_com} id={ok_id} pw={ok_pw}")

    submitted = False
    last_err = None
    for sel in selectors["submit"]:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                loc.click(timeout=5000)
                submitted = True
                break
        except Exception as e:
            last_err = e

    if not submitted:
        dbg_steps.append({"type": "login_submit_fail", "last_err": repr(last_err), "snap": snap_text(page, "login_submit_fail")})
        raise RuntimeError(f"[LOGIN_SUBMIT_FAIL] last_err={last_err}")

    page.wait_for_load_state("domcontentloaded", timeout=30000)
    time.sleep(1.0)
    dbg_steps.append({"type": "snap", "snap": snap_text(page, "after_login_submit")})


def ensure_erp_loaded(page, dbg_steps: list):
    is_login = False
    try:
        if page.locator("text=로그인").count() > 0 and page.locator('input[type="password"]').count() > 0:
            is_login = True
    except Exception:
        pass

    if is_login:
        dbg_steps.append({"type": "info", "msg": "login_page_detected"})
        do_login(page, dbg_steps)

    # ERP 메뉴 확인
    try:
        wait_text_anywhere(page, "재고", 30000, dbg_steps)
        dbg_steps.append({"type": "info", "msg": "erp_menu_found", "snap": snap_text(page, "erp_menu_found")})
    except Exception:
        wait_text_anywhere(page, "판매현황", 30000, dbg_steps)
        dbg_steps.append({"type": "info", "msg": "erp_menu_found_sales", "snap": snap_text(page, "erp_menu_found_sales")})


def run_click_sequence(page, dbg_steps: list):
    dbg_steps.append({"type": "snap", "snap": snap_text(page, "before_sequence")})

    # ✅ 사용자 지정 “필수” 클릭 순서
    seq = ["재고 I", "판매현황", "SAT", "금월(~오늘)", "Excel(화면)"]

    for t in seq:
        click_text_anywhere(page, t, timeout_ms=50000, dbg_steps=dbg_steps)
        time.sleep(1.0)

    dbg_steps.append({"type": "snap", "snap": snap_text(page, "after_sequence_done")})


def run_erp_job():
    missing = require_env()
    if missing:
        return {"ok": False, "error": "Missing env vars", "need": missing}

    dbg_steps = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            context = browser.new_context()
            page = context.new_page()

            ensure_erp_loaded(page, dbg_steps)
            run_click_sequence(page, dbg_steps)

            browser.close()

        return {
            "ok": True,
            "app_rev": APP_REV,
            "stage": "erp_click_sequence_done",
            "timestamp": now_kst_str(),
            "debug": {"steps": dbg_steps},
        }

    except Exception as e:
        dbg_steps.append({
            "type": "exception",
            "err": repr(e),
            "trace": traceback.format_exc(),
        })
        return {
            "ok": False,
            "app_rev": APP_REV,
            "stage": "erp_click_sequence_error",
            "timestamp": now_kst_str(),
            "error": repr(e),
            "debug": {"steps": dbg_steps},
        }


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
            "stages": ["env", "erp"],
            "examples": ["/run?stage=env", "/run?stage=erp"],
            "timestamp": now_kst_str(),
        }), 200

    if stage == "env":
        return jsonify({
            "ok": True,
            "app_rev": APP_REV,
            "env": {
                "ECOUNT_COM_CODE": env_flag("ECOUNT_COM_CODE"),
                "ECOUNT_USER_ID": env_flag("ECOUNT_USER_ID"),
                "ECOUNT_USER_PW": env_flag("ECOUNT_USER_PW"),
            },
            "timestamp": now_kst_str(),
        }), 200

    if stage == "erp":
        result = run_erp_job()
        return jsonify(result), (200 if result.get("ok") else 500)

    return jsonify({
        "ok": False,
        "app_rev": APP_REV,
        "error": "Unknown stage",
        "stage": stage,
        "timestamp": now_kst_str(),
    }), 400
