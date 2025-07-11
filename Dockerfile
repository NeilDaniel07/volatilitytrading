FROM python:3.9-slim

RUN apt-get update && apt-get install -y build-essential gcc

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "executor.py"]
