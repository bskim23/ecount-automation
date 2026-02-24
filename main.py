APP_REV = "2026-02-24_02"  # 아무 문자열로 매번 바꾸세요

from flask import Flask, request, jsonify
import os, json, base64, re, time, datetime
from typing import Dict, Any, Tuple, List

import gspread
from google.oauth2.service_account import Credentials

# ERP 단계에서만 사용 (설치/브라우저 준비 안 됐으면 예외를 잡아 안내)
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
    PLAYWRIGHT_IMPORT_OK = True
except Exception:
    PLAYWRIGHT_IMPORT_OK = False


app = Flask(__name__)


# -----------------------------
# 유틸
# -----------------------------
def now_kst_str() -> str:
    # Cloud Run은 보통 UTC이므로 문자열은 KST로 보기 좋게
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
    """
    GOOGLE_SERVICE_ACCOUNT_JSON:
      - JSON 문자열 그대로
      - 또는 base64 인코딩 문자열
    둘 다 지원.
    """
    raw = get_env("GOOGLE_SERVICE_ACCOUNT_JSON").strip()
    if not raw:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is empty")

    # base64 시도
    if raw.startswith("{") and raw.endswith("}"):
        return json.loads(raw)

    # base64 문자열일 가능성
    try:
        decoded = base64.b64decode(raw).decode("utf-8")
        decoded = decoded.strip()
        if decoded.startswith("{") and decoded.endswith("}"):
            return json.loads(decoded)
    except Exception:
        pass

    # 마지막으로 JSON 로드 시도(개행/이스케이프 깨짐 케이스)
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
    """
    테스트 로그를 남길 워크시트.
    없으면 생성. 있으면 사용.
    """
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


# -----------------------------
# Stage 1: 환경변수 점검
# -----------------------------
def stage_env() -> Dict[str, Any]:
    required = [
        "GOOGLE_SHEET_ID",
        "SHEET_NAME",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "COM_CODE",
        "USER_ID",
        "USER_PW",
    ]
    present = {}
    missing = []
    for k in required:
        v = get_env(k)
        if not v:
            missing.append(k)
            present[k] = {"present": False, "preview": ""}
        else:
            preview = ""
            if k in ("USER_PW",):
                preview = mask(v, keep=1)
            elif k in ("GOOGLE_SERVICE_ACCOUNT_JSON",):
                preview = f"len={len(v)}"
            elif k in ("GOOGLE_SHEET_ID",):
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


# -----------------------------
# Stage 2: 구글시트 쓰기 테스트
# -----------------------------
def stage_gsheet() -> Dict[str, Any]:
    gc = gspread_client()
    sh, ws = open_target_worksheet(gc)
    log_ws = ensure_log_worksheet(sh)

    # 시트 접근 확인 + 로그 1줄
    detail = f"target={ws.title}, sheet_id={sh.id}"
    append_log_row(log_ws, "gsheet", "OK", detail)

    # 안전하게 'Run Log'에만 기록( SAT Raw 본문 건드리지 않음 )
    return {
        "stage": "gsheet",
        "ok": True,
        "message": "Google Sheet write test OK (logged to Run Log)",
        "target_sheet": ws.title,
        "log_sheet": log_ws.title,
        "timestamp": now_kst_str(),
    }


# -----------------------------
# Stage 3: ERP 다운로드 + 엑셀 검증 (Playwright)
# -----------------------------
EXPECTED_HEADERS = [
    "일자-No.",
    "품목명(규격)",
    "수량",
    "단가",
    "공급가액",
    "부가세",
    "합계",
    "거래처명",
    "적요",
    "거래처계층그룹명",
]


def detect_month_key_from_rows(rows: List[List[Any]]) -> str:
    """
    rows: A~J 데이터(헤더 제외)
    A열(일자-No.)에서 YYYY-MM 또는 YYYY/MM 파싱
    """
    for r in rows:
        if not r or len(r) < 1:
            continue
        a = str(r[0]).strip()
        # 예: 2026-02-24-001, 2026/02/24-001 등
        m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", a)
        if m:
            return f"{m.group(1)}/{m.group(2)}"
    # fallback: 현재월
    now = datetime.datetime.now()
    return f"{now.year:04d}/{now.month:02d}"


def ecount_download_and_validate() -> Tuple[bool, Dict[str, Any]]:
    if not PLAYWRIGHT_IMPORT_OK:
        return False, {
            "error": "Playwright import failed",
            "hint": "requirements에 playwright가 있어도 런타임/이미지에 브라우저가 준비되지 않으면 동작하지 않습니다. (아래 오류 로그를 확인하세요)",
        }

    com_code = get_env("COM_CODE").strip()
    user_id = get_env("USER_ID").strip()
    user_pw = get_env("USER_PW").strip()

    login_url = get_env("ECOUNT_LOGIN_URL", "https://login.ecount.com/Login/").strip()
    headless = get_env("HEADLESS", "true").lower() != "false"
    timeout_ms = int(get_env("ECOUNT_TIMEOUT_MS", "60000"))  # 60s
    nav_timeout_ms = int(get_env("ECOUNT_NAV_TIMEOUT_MS", "60000"))

    # 다운로드 저장 경로 (Cloud Run에서는 /tmp 사용)
    dl_dir = get_env("DOWNLOAD_DIR", "/tmp").strip() or "/tmp"

    result: Dict[str, Any] = {
        "login_url": login_url,
        "headless": headless,
        "download_dir": dl_dir,
    }

    try:
        from openpyxl import load_workbook
    except Exception as e:
        return False, {"error": f"openpyxl import failed: {repr(e)}"}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.set_default_navigation_timeout(nav_timeout_ms)

            # 1) 로그인 페이지
            page.goto(login_url, wait_until="domcontentloaded")

            # 2) 입력 필드 탐색 (가급적 안전한 방식: name/id 기반 + fallback)
            # 회사코드
            # - 사이트 구조가 바뀔 수 있어 "가장 그럴듯한 input"을 찾는 로직(단순화)
            def fill_best(label_keywords: List[str], value: str):
                inputs = page.locator("input")
                count = inputs.count()
                best_idx = None
                best_score = -1
                for i in range(count):
                    el = inputs.nth(i)
                    try:
                        attrs = " ".join([
                            (el.get_attribute("name") or ""),
                            (el.get_attribute("id") or ""),
                            (el.get_attribute("class") or ""),
                            (el.get_attribute("placeholder") or ""),
                            (el.get_attribute("aria-label") or ""),
                        ]).lower()
                    except Exception:
                        continue
                    score = 0
                    for k in label_keywords:
                        if k.lower() in attrs:
                            score += 10
                    # visible 가중
                    try:
                        if el.is_visible():
                            score += 3
                    except Exception:
                        pass
                    if score > best_score:
                        best_score = score
                        best_idx = i

                if best_idx is None or best_score <= 0:
                    # fallback: 첫 번째 visible input
                    for i in range(count):
                        el = inputs.nth(i)
                        try:
                            if el.is_visible():
                                best_idx = i
                                break
                        except Exception:
                            continue

                if best_idx is None:
                    raise RuntimeError(f"no input found for {label_keywords}")

                el = inputs.nth(best_idx)
                el.click()
                el.fill(value)

            # password는 type=password를 우선
            try:
                # 회사코드 / 아이디
                fill_best(["com", "company", "회사", "코드", "code"], com_code)
                fill_best(["id", "user", "login", "아이디"], user_id)

                pw = page.locator("input[type='password']").first
                pw.click()
                pw.fill(user_pw)
            except Exception as e:
                result["fill_error"] = repr(e)
                # 다음을 위해 화면 상태를 유지한 채 진행 시도
                pass

            # 로그인 버튼
            # '로그인' 텍스트 기반
            btn = page.get_by_role("button", name=re.compile(r"(로그인|login)", re.IGNORECASE))
            if btn.count() > 0:
                btn.first.click()
            else:
                # submit fallback
                page.keyboard.press("Enter")

            # 3) 메뉴 이동 (판매현황 → SAT → 금월(~오늘) → Excel(화면))
            # 로딩 대기
            page.wait_for_timeout(2000)

            def click_text_exact(txt: str):
                # 텍스트를 포함하는 요소들 중 exact에 가깝게 클릭
                loc = page.locator(f"text={txt}")
                if loc.count() > 0:
                    loc.first.click()
                    return True
                return False

            ok_steps = {}
            ok_steps["판매현황"] = click_text_exact("판매현황")
            page.wait_for_timeout(1500)
            ok_steps["SAT"] = click_text_exact("SAT")
            page.wait_for_timeout(1500)
            ok_steps["금월(~오늘)"] = click_text_exact("금월(~오늘)")
            page.wait_for_timeout(1500)

            # 4) 다운로드
            # "Excel(화면)" 또는 변형
            excel_clicked = False
            for label in ["Excel(화면)", "엑셀(화면)", "Excel"]:
                if click_text_exact(label):
                    excel_clicked = True
                    break

            ok_steps["ExcelClick"] = excel_clicked

            if not excel_clicked:
                raise RuntimeError("Excel download button not found")

            # 다운로드 이벤트 대기
            with page.expect_download(timeout=timeout_ms) as dlinfo:
                # 클릭이 이미 됐으면 다운로드가 즉시 시작될 수 있어 잠깐 대기
                pass
            download = dlinfo.value
            suggested = download.suggested_filename
            save_path = os.path.join(dl_dir, suggested)
            download.save_as(save_path)

            result["downloaded_file"] = save_path

            # 5) 엑셀 검증
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

            # 데이터 수집 (A~J, 3행부터, 마지막 3행 제외)
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
            result["steps"] = ok_steps

            browser.close()

        return True, result

    except PWTimeoutError as e:
        return False, {"error": f"Playwright timeout: {repr(e)}", "partial": result}
    except Exception as e:
        # Playwright에서 흔히 발생: 브라우저 미설치, 샌드박스 권한 등
        return False, {
            "error": f"ERP stage failed: {repr(e)}",
            "partial": result,
            "hint": "Cloud Run에서 Playwright는 '브라우저/의존성'이 이미지에 포함돼야 안정적으로 동작합니다. (빌드팩만으로는 막히는 경우가 흔합니다)",
        }


def stage_erp() -> Dict[str, Any]:
    ok, payload = ecount_download_and_validate()
    return {
        "stage": "erp",
        "ok": ok,
        "payload": payload,
        "timestamp": now_kst_str(),
    }


# -----------------------------
# Stage 4: 전체(all) - 월별 교체 업로드 (A~J)
# -----------------------------
def ym_key_from_a(a_val: Any) -> str:
    if a_val is None:
        return ""
    s = str(a_val).strip()
    m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    # 추가 케이스: YYYYMMDD
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    return ""


def stage_all() -> Dict[str, Any]:
    # 1) env
    env_res = stage_env()
    if not env_res["ok"]:
        return {"stage": "all", "ok": False, "failed_at": "env", "env": env_res, "timestamp": now_kst_str()}

    # 2) gsheet 접근
    gc = gspread_client()
    sh, ws = open_target_worksheet(gc)
    log_ws = ensure_log_worksheet(sh)

    # 3) ERP 다운로드/검증 + 데이터 확보
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

    # 엑셀 다시 열어 rows(A~J) 재수집 (read_only로 이미 했지만 여기서 확실히)
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

    # 4) 대상 시트 기존 데이터 가져오기
    #    - A~J 범위만 다룸 (K~M은 시트 수식으로 계산되게 둠: 선택 B)
    values = ws.get_all_values()  # 텍스트로 가져옴
    if not values:
        values = []

    header = values[0] if values else []
    body = values[1:] if len(values) >= 2 else []

    # 헤더가 없다면 기본 헤더를 만들어두는 선택지도 있음(여기선 그대로 둠)
    kept = []
    deleted = 0
    for r in body:
        # 최소 A열 존재 가정
        a = r[0] if len(r) > 0 else ""
        if ym_key_from_a(a) == month_key:
            deleted += 1
        else:
            kept.append(r)

    # 5) 새 body 구성
    # rows는 파이썬 값(숫자/None 등)이 섞여있으니 "USER_ENTERED"로 업데이트
    # 구글시트에 들어갈 A~J를 문자열/숫자 그대로 넘김
    new_body = kept + rows

    # 6) 시트에 반영 (A~J만)
    # 기존 내용을 지우고 다시 쓰는 방식(월 단위 교체 + 누적 유지)
    # - 성능 이슈가 생기면 batchUpdate로 최적화 가능
    # - 여기서는 명확성이 우선
    ws.clear()

    out = []
    if header:
        out.append(header)
    else:
        # header가 없으면 소스 헤더를 넣어도 되지만, 기존 시트 구조를 존중하기 위해 비움
        pass

    # out에는 문자열/숫자 혼합 가능. gspread update는 가능.
    if out:
        out.extend(new_body)
        ws.update("A1", out, value_input_option="USER_ENTERED")
    else:
        # 헤더가 없으면 1행부터 데이터
        ws.update("A1", new_body, value_input_option="USER_ENTERED")

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


# -----------------------------
# 라우팅
# -----------------------------
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
            "examples": [
                "/run?stage=env",
                "/run?stage=gsheet",
                "/run?stage=erp",
                "/run?stage=all"
            ],
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
