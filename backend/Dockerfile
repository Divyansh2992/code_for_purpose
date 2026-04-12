FROM python:3.11-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  PYSPARK_PYTHON=python3 \
  PYSPARK_DRIVER_PYTHON=python3 \
  SPARK_LOCAL_IP=127.0.0.1 \
  SPARK_LOCAL_HOSTNAME=localhost

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
  openjdk-17-jre-headless \
  tini \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

ENTRYPOINT ["tini", "--"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
