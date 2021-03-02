FROM python:3.8

RUN pip install pipenv

ENV PYTHONUNBUFFERED 1
ENV API_DIR /app

WORKDIR ${API_DIR}

COPY Pipfile .
COPY Pipfile.lock .
COPY geckodriver /usr/local/bin

COPY . .

RUN pipenv install --deploy --ignore-pipfile
RUN pipenv install hdfs
RUN pipenv install selenium

RUN apt-get install firefox

EXPOSE 5000

CMD ["pipenv", "run", "python", "api.py"]


