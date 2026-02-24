FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ✅ greenlet(네이티브) + playwright 실행에 필요한 기본 도구/라이브러리
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ make \
    curl ca-certificates \
    # playwright 런타임에서 종종 요구되는 라이브러리(보수적으로 포함)
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpangocairo-1.0-0 libpango-1.0-0 libcairo2 \
    libgtk-3-0 \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ 브라우저 설치 (chromium)
RUN python -m playwright install chromium

COPY . .

EXPOSE 8080
CMD ["gunicorn", "-b", ":8080", "main:app", "--workers", "1", "--threads", "8", "--timeout", "300"]
