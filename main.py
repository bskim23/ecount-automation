from flask import Flask, jsonify
import os

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health():
    return "OK", 200

@app.route("/run", methods=["GET"])
def run_job():
    keys = [
        "GOOGLE_SHEET_ID",
        "SHEET_NAME",
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "COM_CODE",
        "USER_ID",
        "USER_PW",
        "ENV",
    ]
    present = {k: bool(os.environ.get(k)) for k in keys}
    # 민감정보는 값 자체를 절대 출력하지 않고 존재 여부만 체크
    return jsonify({
        "status": "env_check",
        "present": present
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

# -------------------------
# 0) Health
# -------------------------
@app.get("/")
def health():
    return "OK", 200


# -------------------------
# 1) Helpers
# -------------------------
def env_required(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        raise RuntimeError(f"Missing env: {key}")
    return v

def norm(s: Any) -> str:
    return ("" if s is None else str(s)).strip()

def ym_key_from_value(v: Any) -> str:
    """
    Google Sheet A열의 값이 날짜/문자 어떤 형태든 YYYY/MM로 정규화.
    """
    s = norm(v)
    if not s:
        return ""
    # YYYY/MM, YYYY-MM, YYYY.MM
    m = re.match(r"^(\d{4})\s*[/\-\.]\s*(\d{1,2})", s)
    if m:
        return f"{m.group(1)}/{int(m.group(2)):02d}"
    # YYYY년 M월
    m = re.match(r"^(\d{4})\s*년\s*(\d{1,2})\s*월", s)
    if m:
        return f"{m.group(1)}/{int(m.group(2)):02d}"
    # YYYYMMDD
    m = re.match(r"^(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    # 혹시 날짜 문자열(YYYY-MM-DD...)이면 앞 7글자 처리
    m = re.match(r"^(\d{4})-(\d{2})", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    return ""

def month_key_now() -> str:
    now = datetime.datetime.now()
    return f"{now.year:04d}/{now.month:02d}"


# -------------------------
# 2) Google Sheets auth
# -------------------------
def gspread_client():
    sa_json = env_required("GOOGLE_SA_JSON")  # 서비스계정 JSON 전체(문자열)
    info = json.loads(sa_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


# -------------------------
# 3) Playwright: Ecount download
# -------------------------
def click_exact_text(page, text: str, timeout_ms: int = 15000) -> bool:
    try:
        loc = page.get_by_text(text, exact=True)
        loc.first.wait_for(timeout=timeout_ms)
        loc.first.click()
        return True
    except Exception:
        return False

def ecount_download_xlsx(tmp_dir: str) -> str:
    """
    이카운트 UI 자동 클릭으로 xlsx 다운로드.
    다운로드 버튼 텍스트는 실제 화면에 따라 1~2개만 튜닝하면 됩니다.
    """
    com_code = env_required("ECOUNT_COM_CODE")
    user_id = env_required("ECOUNT_USER_ID")
    user_pw = env_required("ECOUNT_USER_PW")

    out_path = os.path.join(tmp_dir, f"ecount_sales_{int(time.time())}.xlsx")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto("https://login.ecount.com/Login/", wait_until="domcontentloaded")

        # 로그인 폼은 DOM이 바뀔 수 있어 "기본 입력"을 먼저 시도 후, 필요하면 셀렉터를 조정합니다.
        # 1) password
        try:
            page.locator("input[type='password']").first.fill(user_pw)
        except Exception:
            pass

        # 2) text inputs: 회사코드/아이디 추정 (순서가 바뀌면 여기만 조정)
        inputs = page.locator("input")
        try:
            # 경험상 회사코드/아이디가 앞에 오는 경우가 많습니다.
            inputs.nth(0).fill(com_code)
            inputs.nth(1).fill(user_id)
        except Exception:
            pass

        # 로그인
        if not click_exact_text(page, "로그인") and not click_exact_text(page, "Login"):
            page.keyboard.press("Enter")

        page.wait_for_timeout(3000)

        # 메뉴 이동 (텍스트가 다르면 여기만 튜닝)
        click_exact_text(page, "재고 I")
        page.wait_for_timeout(1500)

        click_exact_text(page, "판매현황")
        page.wait_for_timeout(1500)

        click_exact_text(page, "SAT")
        page.wait_for_timeout(1500)

        click_exact_text(page, "금월(~오늘)")
        page.wait_for_timeout(1500)

        # 다운로드 트리거: "Excel(화면)" 또는 "엑셀(화면)" 등
        downloaded = False
        candidates = ["Excel(화면)", "엑셀(화면)", "Excel", "엑셀"]
        for t in candidates:
            try:
                with page.expect_download(timeout=20000) as dlinfo:
                    if click_exact_text(page, t):
                        download = dlinfo.value
                        download.save_as(out_path)
                        downloaded = True
                        break
            except Exception:
                pass

        browser.close()

    if not downloaded or not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        raise RuntimeError("Download failed: xlsx not downloaded")

    return out_path


# -------------------------
# 4) Excel parse (판매현황 A~J)
# -------------------------
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

def parse_sales_rows(xlsx_path: str) -> List[List[Any]]:
    wb = load_workbook(xlsx_path, data_only=False, read_only=True)
    if "판매현황" not in wb.sheetnames:
        raise RuntimeError(f'No sheet "판매현황": {wb.sheetnames}')
    ws = wb["판매현황"]

    headers = [ws.cell(row=2, column=c).value for c in range(1, 11)]
    if headers != EXPECTED_HEADERS:
        raise RuntimeError("Header mismatch in 판매현황 (row2 A~J)")

    # 마지막 행 탐색
    last = ws.max_row
    while last >= 3 and ws.cell(row=last, column=1).value in (None, ""):
        last -= 1

    data_end = last - 3  # 마지막 3행 제외(기존 로직 유지)
    if data_end < 3:
        raise RuntimeError("No data rows after excluding last 3 rows")

    rows: List[List[Any]] = []
    for r in range(3, data_end + 1):
        row = [ws.cell(row=r, column=c).value for c in range(1, 11)]
        rows.append(row)

    return rows


# -------------------------
# 5) Google Sheet upsert (월별 덮어쓰기 + 누적 유지)
# -------------------------
def build_k_formula(row: int) -> str:
    # k2=LEFT(A2,10)
    return f'=LEFT(A{row},10)'

def build_l_formula(row: int) -> str:
    # l2=IFERROR(DATEVALUE(TEXT(K2, "yyyy-mm-dd")), )
    return f'=IFERROR(DATEVALUE(TEXT(K{row}, "yyyy-mm-dd")), )'

def build_m_formula(row: int) -> str:
    # 사용자 제공 수식 (B열 기준)
    return (
        f'=IF(ISNUMBER(SEARCH("오스모",B{row})),"오스모타이트",'
        f'IF(ISNUMBER(SEARCH("롤링",B{row})),"롤링핑거",'
        f'IF(ISNUMBER(SEARCH("허브",B{row})),"허브온팩",'
        f'IF(ISNUMBER(SEARCH("허리",B{row})),"허리온팩",'
        f'IF(ISNUMBER(SEARCH("아이온",B{row})),"아이온팩",'
        f'IF(ISNUMBER(SEARCH("블랙홀",B{row})),"블랙홀파스",'
        f'IF(ISNUMBER(SEARCH("스키놀로",B{row})),"스키놀로지",'
        f'IF(ISNUMBER(SEARCH("스윙",B{row})),"스윙",'
        f'IF(ISNUMBER(SEARCH("다리",B{row})),"다리피팅","기타")))))))))'
    )

def update_sheet_month(rows_aj: List[List[Any]]) -> dict:
    sh_id = env_required("GSHEET_ID")
    ws_name = env_required("GSHEET_TAB")  # 예: "SAT Raw"
    month_key = month_key_now()

    gc = gspread_client()
    sh = gc.open_by_key(sh_id)
    ws = sh.worksheet(ws_name)

    # 기존 A열 데이터(2행부터) 읽기
    colA = ws.col_values(1)  # 1-index, includes header rows
    # 데이터 시작 행: 2행부터라고 가정 (1행 헤더)
    start_data_row = 2

    # 실제 마지막 행
    last_row = len(colA)
    if last_row < start_data_row:
        last_row = start_data_row - 1

    # 월별 행 인덱스 수집
    month_rows = []
    for r in range(start_data_row, last_row + 1):
        k = ym_key_from_value(colA[r - 1])  # col_values는 0-index
        if k == month_key:
            month_rows.append(r)

    deleted = 0
    insert_at = None

    # 1) 해당월 구간 삭제
    if month_rows:
        first_r = month_rows[0]
        last_r = month_rows[-1]
        ws.delete_rows(first_r, last_r)
        deleted = (last_r - first_r + 1)
        insert_at = first_r
    else:
        # 2) 삽입 위치 결정: YYYY/MM 기준으로 다음 달 이전
        insert_at = last_row + 1
        for r in range(start_data_row, last_row + 1):
            k = ym_key_from_value(colA[r - 1])
            if k and k > month_key:
                insert_at = r
                break

    # 3) A~J 삽입 (RAW)
    ws.insert_rows(rows_aj, row=insert_at, value_input_option="RAW")
    inserted = len(rows_aj)

    # 4) K~M 수식 채우기 (USER_ENTERED)
    # 삽입 후 행 번호 기준
    if inserted > 0:
        k_range = f"K{insert_at}:K{insert_at + inserted - 1}"
        l_range = f"L{insert_at}:L{insert_at + inserted - 1}"
        m_range = f"M{insert_at}:M{insert_at + inserted - 1}"

        k_vals = [[build_k_formula(r)] for r in range(insert_at, insert_at + inserted)]
        l_vals = [[build_l_formula(r)] for r in range(insert_at, insert_at + inserted)]
        m_vals = [[build_m_formula(r)] for r in range(insert_at, insert_at + inserted)]

        ws.update(k_range, k_vals, value_input_option="USER_ENTERED")
        ws.update(l_range, l_vals, value_input_option="USER_ENTERED")
        ws.update(m_range, m_vals, value_input_option="USER_ENTERED")

    return {
        "month": month_key,
        "deleted_rows": deleted,
        "inserted_rows": inserted,
        "insert_at": insert_at,
        "tab": ws_name,
    }


# -------------------------
# 6) /run endpoint
# -------------------------
@app.route("/run", methods=["POST", "GET"])
def run_job():
    # (권장) 간단 보호: RUN_TOKEN 헤더 일치 시만 실행
    # Cloud Scheduler 호출에도 그대로 적용 가능
    run_token = os.environ.get("RUN_TOKEN")
    if run_token:
        got = request.headers.get("X-Run-Token", "")
        if got != run_token:
            return jsonify({"status": "denied"}), 403

    try:
        tmp_dir = "/tmp"
        xlsx_path = ecount_download_xlsx(tmp_dir)
        rows_aj = parse_sales_rows(xlsx_path)
        result = update_sheet_month(rows_aj)

        return jsonify({
            "status": "ok",
            "downloaded": os.path.basename(xlsx_path),
            "row_count": len(rows_aj),
            "sheet_update": result,
        }), 200

    except Exception as e:
        return jsonify({
            "status": "fail",
            "error": str(e),
        }), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
