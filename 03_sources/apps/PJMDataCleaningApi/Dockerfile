FROM python:3.8

RUN pip install pipenv

ENV PYTHONUNBUFFERED 1
ENV API_DIR /app

WORKDIR ${API_DIR}


COPY . .

RUN pipenv install --deploy --ignore-pipfile
RUN pipenv install hdfs
RUN pipenv install cassandra


EXPOSE 5002

CMD ["pipenv", "run", "python", "api.py"]


