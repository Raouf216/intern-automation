FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN python -m pip install --root-user-action=ignore --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "-u", "/app/main.py"]
