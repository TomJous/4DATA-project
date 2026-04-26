FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt &&\
    pip install --disable-pip-version-check --no-cache-dir -e .[dev]

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0"]