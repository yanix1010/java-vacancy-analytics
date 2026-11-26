FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "boto3>=1.43,<2"

COPY mapreduce_engine.py /app/
COPY mapreduce_k8s.py /app/

CMD ["python", "-u", "mapreduce_k8s.py", "--sizes", "64"]
