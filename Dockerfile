
FROM python:3.9-alpine
#is this what you want or you want carepay to be your main directory
COPY  ./carepay_etl ./carepay_etl
COPY carepay_etl/requirements.txt /tmp
RUN pip3 install -r /tmp/requirements.txt
WORKDIR /carepay_etl
CMD ["python3", "main.py"]
