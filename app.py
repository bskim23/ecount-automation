import os
import json
import base64
import datetime
from flask import Flask, request, jsonify
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

APP_REV = "2026-02-24_erp_click_sequence_v1"

# ====== ENV ======
ECOUNT_COM_CODE = os.getenv("ECOUNT_COM_CODE", "")
ECOUNT_USER_ID   = os.getenv("ECOUNT_USER_ID", "")
ECOUNT_USER_PW   = os.getenv("ECOUNT_USER_PW", "")

# ====== Flask ======
app = Flask(__name__)

def now_kst_str():
    # Asia/Seoul fixed offset (+09:00)
    return (datetime.datetime.utcnow() + datetime.timedelta(hours=9)).strftime("%Y-%m-%d %H:%M:%S+0900")

def need_env():
    need = []
    if not ECOUNT_COM_CODE: need.append("ECOUNT_COM_CODE")
    if not ECOUNT_USER_ID:  need.append("ECOUNT_USER_ID")
    if not ECOUNT_USER_PW:  need.append("ECOUNT_USER_PW")
    return need

def b64_screenshot(page):
    try:
        png = page.screenshot(full_page=True)
        return base64.b64encode(png).decode("utf-8")
    except Exception:
        return ""

def collect_console_tail(console_buffer, n=60):
    return console_buffer[-n:] if len(console_buffer) > n else console_buffer

def frame_selector_counts(frame):
    # 필요하면 여기서 각 selector 존재 여부 디버그
    def cnt(sel):
        try:
            return frame.locator(sel).count()
        except Exception:
            return 0
    return {
        "inv": cnt("text=재고 I"),
        "sales_status": cnt("text=판매현황"),
        "sat": cnt("text=SAT"),
        "this_month": cnt("text=금월(~오늘)"),
        "search": cnt("text=조회"),
        "excel": cnt("text=엑셀"),
    }

def dump_frames_state(page):
    frames = []
    for i, fr in enumerate(page.frames):
        try:
            frames.append({
                "i": i,
                "name": fr.name or "",
                "url": fr.url,
                "readyState": fr.evaluate("() => document.readyState") if fr.url.startswith("http") else "n/a",
                "selector_counts": frame_selector_counts(fr),
            })
        except Exception:
            frames.append({
                "i": i,
                "name": fr.name or "",
                "url": fr.url,
                "readyState": "n/a",
                "selector_counts": {},
            })
    return {
        "page_url": page.url,
        "frames": frames,
    }

def click_text_anywhere(page, text, timeout_ms=12000):
    """
    page 및 모든 frame에서 주어진 텍스트를 찾아 클릭.
    텍스트는 UI 상 보이는 라벨 기준.
    """
    # 1) page main 먼저
    try:
        loc = page.get_by_text(text, exact=True)
        if loc.count() > 0:
            loc.first.click(timeout=timeout_ms)
            return {"ok": True, "where": "page", "text": text}
    except Exception:
        pass

    # 2) frames 순회
    for fr in page.frames:
        try:
            if not fr.url or fr.url.startswith("about:"):
                continue
            loc = fr.get_by_text(text, exact=True)
            if loc.count() > 0:
                loc.first.click(timeout=timeout_ms)
                return {"ok": True, "where": f"frame:{fr.name or fr.url}", "text": text}
        except Exception:
            continue

    # 3) partial match fallback
    try:
        loc = page.get_by_text(text, exact=False)
        if loc.count() > 0:
            loc.first.click(timeout=timeout_ms)
            return {"ok": True, "where": "page(partial)", "text": text}
    except Exception:
        pass

    for fr in page.frames:
        try:
            if not fr.url or fr.url.startswith("about:"):
                continue
            loc = fr.get_by_text(text, exact=False)
            if loc.count() > 0:
                loc.first.click(timeout=timeout_ms)
                return {"ok": True, "where": f"frame(partial):{fr.name or fr.url}", "text": text}
        except Exception:
            continue

    return {"ok": False, "where": None, "text": text}

def try_click_candidates(page, candidates, timeout_ms=8000):
    for t in candidates:
        r = click_text_anywhere(page, t, timeout_ms=timeout_ms)
        if r["ok"]:
            return {"ok": True, "picked": t, "detail": r}
    return {"ok": False, "picked": None}

def login_ecount(page, console_buffer):
    # 로그인 페이지
    page.goto("https://login.ecount.com/", wait_until="domcontentloaded", timeout=60000)

    # 회사코드/ID/PW 입력
    page.fill('input[name="comCode"], input#comCode, input[placeholder*="회사"], input[placeholder*="회사코드"]', ECOUNT_COM_CODE)
    page.fill('input[name="id"], input#id, input[placeholder*="아이디"], input[placeholder*="ID"]', ECOUNT_USER_ID)
    page.fill('input[name="passwd"], input#passwd, input[type="password"]', ECOUNT_USER_PW)

    # 로그인 버튼(텍스트/타입 다양성 대응)
    # 가능한 버튼 후보
    btn_candidates = [
        "로그인", "LOGIN", "Login"
    ]
    clicked = False
    for t in btn_candidates:
        try:
            loc = page.get_by_text(t, exact=False)
            if loc.count() > 0:
                loc.first.click(timeout=8000)
                clicked = True
                break
        except Exception:
            pass

    if not clicked:
        # submit fallback
        try:
            page.press('input[type="password"]', "Enter")
        except Exception:
            pass

    # ERP로 이동 (로그인 후 자동 리다이렉트가 있을 수 있어 wait)
    page.wait_for_load_state("networkidle", timeout=60000)

    # ERP 진입 URL이 다를 수 있어, 이미 ERP로 갔는지 확인
    if "ec5/view/erp" not in page.url:
        # ERP 홈으로 직접 이동(기존 로그와 동일한 형태로 진입)
        page.goto("https://loginca.ecount.com/ec5/view/erp?w_flag=1", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)

def run_erp_click_download():
    need = need_env()
    if need:
        return jsonify({"ok": False, "error": "Missing env vars", "need": need}), 400

    console_buffer = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(accept_downloads=True, locale="ko-KR")
        page = context.new_page()

        # console capture
        def on_console(msg):
            try:
                console_buffer.append({"type": msg.type, "text": msg.text})
            except Exception:
                pass
        page.on("console", on_console)

        debug = {}
        try:
            login_ecount(page, console_buffer)
            debug["erp_loaded"] = {
                "page_url": page.url,
                "frames_state": dump_frames_state(page),
                "console_tail": collect_console_tail(console_buffer),
                "screenshot_b64": b64_screenshot(page),
            }

            # ====== (1) 클릭 시퀀스 ======
            click_steps = [
                "재고 I",
                "판매현황",
                "SAT",
                "금월(~오늘)",
            ]

            clicked_seq = []
            for t in click_steps:
                r = click_text_anywhere(page, t, timeout_ms=15000)
                clicked_seq.append(r)
                # 클릭 직후 UI가 바뀌는 경우 대비
                page.wait_for_timeout(800)

            debug["click_sequence"] = clicked_seq
            debug["after_click_frames_state"] = dump_frames_state(page)
            debug["after_click_screenshot_b64"] = b64_screenshot(page)

            # ====== (2) 조회 버튼(있으면) ======
            search_try = try_click_candidates(page, ["조회", "검색", "Search"], timeout_ms=8000)
            debug["search_try"] = search_try
            if search_try["ok"]:
                page.wait_for_load_state("networkidle", timeout=60000)

            # ====== (3) 엑셀 다운로드(있으면) ======
            # “엑셀”, “EXCEL”, “엑셀다운로드”, “다운로드” 등 화면별 차이 흡수
            download_candidates = ["엑셀", "EXCEL", "Excel", "엑셀다운로드", "다운로드", "내려받기"]
            dl_result = {"ok": False}

            # 다운로드 이벤트를 먼저 걸고 클릭
            for t in download_candidates:
                try:
                    with page.expect_download(timeout=15000) as dl_info:
                        r = click_text_anywhere(page, t, timeout_ms=8000)
                        if not r["ok"]:
                            continue
                    download = dl_info.value
                    # 저장
                    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    save_path = f"/tmp/ecount_{ts}_{download.suggested_filename}"
                    download.save_as(save_path)
                    dl_result = {"ok": True, "clicked": t, "file": {"path": save_path, "name": download.suggested_filename}}
                    break
                except PWTimeout:
                    continue
                except Exception:
                    continue

            debug["download"] = dl_result
            debug["final_screenshot_b64"] = b64_screenshot(page)

            return jsonify({
                "ok": True,
                "app_rev": APP_REV,
                "timestamp": now_kst_str(),
                "page_url": page.url,
                "debug": debug,
            }), 200

        except Exception as e:
            debug["error"] = str(e)
            debug["frames_state"] = dump_frames_state(page)
            debug["console_tail"] = collect_console_tail(console_buffer)
            debug["screenshot_b64"] = b64_screenshot(page)
            return jsonify({
                "ok": False,
                "app_rev": APP_REV,
                "timestamp": now_kst_str(),
                "error": str(e),
                "debug": debug,
            }), 500
        finally:
            context.close()
            browser.close()

@app.route("/", methods=["GET"])
def root():
    return "OK", 200

@app.route("/run", methods=["GET"])
def run_job():
    stage = (request.args.get("stage") or "").strip().lower()
    if stage in ("", "help"):
        return jsonify({
            "ok": True,
            "app_rev": APP_REV,
            "stages": ["env", "erp"],
            "examples": [
                "/run?stage=env",
                "/run?stage=erp",
            ],
            "timestamp": now_kst_str(),
        }), 200

    if stage == "env":
        return jsonify({
            "ok": True,
            "app_rev": APP_REV,
            "timestamp": now_kst_str(),
            "env": {
                "ECOUNT_COM_CODE": "✅" if bool(ECOUNT_COM_CODE) else "❌",
                "ECOUNT_USER_ID":  "✅" if bool(ECOUNT_USER_ID) else "❌",
                "ECOUNT_USER_PW":  "✅" if bool(ECOUNT_USER_PW) else "❌",
            }
        }), 200

    if stage == "erp":
        return run_erp_click_download()

    return jsonify({"ok": False, "error": "Unknown stage", "stage": stage, "app_rev": APP_REV}), 400
