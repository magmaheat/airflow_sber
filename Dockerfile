FROM apache/airflow:2.4.1

WORKDIR .

COPY . .

RUN pip install --no-cache-dir -r requirements.txt