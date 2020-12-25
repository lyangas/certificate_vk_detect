FROM python:3

ADD . .

RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get -y install tesseract-ocr-eng
RUN apt-get -y install tesseract-ocr-rus