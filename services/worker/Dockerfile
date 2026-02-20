FROM python:3.9-slim

WORKDIR /worker

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt uvicorn

COPY . .

CMD ["python", "app.py"]
