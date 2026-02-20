FROM python:3.9-slim

WORKDIR /server

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt uvicorn

COPY . .

CMD ["fastapi", "run"]
