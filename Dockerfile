FROM python:3.8.8

WORKDIR /opt/service

ARG ENV=production

COPY poetry.lock pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir poetry==1.1.5 \
  \
  && poetry config virtualenvs.create false \
  && poetry install --no-dev --no-interaction --no-ansi \
  \
  && pip uninstall --yes poetry

COPY . ./

CMD [ "bash" ]