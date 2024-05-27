FROM python:3.10.6-buster as strike-py

ENV PYTHONUNBUFFERED 1

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt 

COPY . .

CMD python manage.py runserver 0.0.0.0:8000

EXPOSE 8000

LABEL org.opencontainers.image.source=https://github.com/ampcome/strike-py
