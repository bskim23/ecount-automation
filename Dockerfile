FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# greenlet(및 기타 native) 대비 + playwright 의존성 대비
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ make \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Playwright 브라우저 설치 (chromium)
# --with-deps 는 OS 의존 패키지를 더 설치하려고 하는데,
# slim에서는 일부 환경에서 실패하는 경우가 있어, 기본 설치로 가는 편이 안정적입니다.
# 필요 시 아래 줄을 --with-deps chromium 으로 바꿔도 됩니다.
RUN python -m playwright install chromium

COPY . .

# Cloud Run은 반드시 PORT를 리슨해야 합니다.
CMD ["gunicorn", "-b", ":8080", "main:app"]
