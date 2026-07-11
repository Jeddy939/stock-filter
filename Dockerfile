FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

COPY requirements-cloud.txt requirements-cloud.txt
RUN pip install --no-cache-dir -r requirements-cloud.txt

COPY src src
COPY web_app.py web_app.py
COPY data_fetcher.py data_fetcher.py
COPY "default filter settings.json" "default filter settings.json"
COPY cloud_backend cloud_backend
COPY firebase firebase
COPY *tickers*.txt .
COPY asx_yfinance_valid_stocks_*.txt .
COPY cloud_entrypoint.py cloud_entrypoint.py

EXPOSE 8080
CMD ["python", "cloud_entrypoint.py"]
