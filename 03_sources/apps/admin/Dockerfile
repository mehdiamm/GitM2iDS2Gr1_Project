FROM python:3.9
ENV PYTHONUNBUFFERED 1
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY . /app

CMD 'python admin.py runserver 0.0.0.0:8000'
