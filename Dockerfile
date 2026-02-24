FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Playwright/Chromium 구동에 필요한 시스템 의존성 + 한글 폰트(선택)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Chromium 설치 (Playwright 권장 방식)
RUN python -m playwright install --with-deps chromium

COPY . .

EXPOSE 8080
CMD ["gunicorn", "-b", ":8080", "main:app", "--workers", "1", "--threads", "8", "--timeout", "300"]
