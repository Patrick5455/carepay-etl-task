FROM python:3.9-alpine
COPY  ./carepay_etl ./carepay_etl
COPY ./requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt
WORKDIR ./carepay_etl
CMD ["python3", "main.py"]
