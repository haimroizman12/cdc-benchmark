FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    freetds-dev freetds-bin gcc && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bench/ ./bench/
ENTRYPOINT ["python", "-m", "bench.run"]
