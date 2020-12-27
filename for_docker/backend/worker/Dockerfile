FROM python:3

ADD ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get -y install tesseract-ocr-eng
RUN apt-get -y install tesseract-ocr-rus

ADD . .

ENTRYPOINT ["python", "-u", "certificat_detect_worker.py"]