FROM python:3.11

WORKDIR /app

COPY etl_job.py .

RUN pip install psycopg2 pandas

CMD ["python", "etl_job.py"]
