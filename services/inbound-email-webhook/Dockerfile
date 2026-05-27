FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PORT=8000
ENV SMTP_HOST=0.0.0.0
ENV SMTP_PORT=2525

COPY requirements.txt .
RUN python -m pip install --root-user-action=ignore --no-cache-dir -r requirements.txt
RUN mkdir -p /app/data

COPY app ./app

EXPOSE 8000 2525

CMD ["python", "-m", "app.main"]
