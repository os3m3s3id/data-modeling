FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY raw_to_silver.py .

CMD ["python", "raw_to_silver.py"]