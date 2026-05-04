FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PORT=8000

COPY requirements.txt .
RUN python -m pip install --root-user-action=ignore --no-cache-dir -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["python", "-m", "app.main"]
