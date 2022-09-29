FROM python:3.10

WORKDIR /app

RUN pip install pipenv

COPY Pipfile.lock Pipfile.lock

RUN pipenv sync

CMD pipenv run python main.py

COPY . /app