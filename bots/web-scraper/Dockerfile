FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PORT=8000

COPY requirements.txt .
RUN python -m pip install --root-user-action=ignore --no-cache-dir -r requirements.txt \
    && python -m playwright install chromium

COPY app ./app

EXPOSE 8000

CMD ["python", "-m", "app.main"]
