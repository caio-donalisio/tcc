# Inspira Crawlers

## Setup

Para gestão de dependências usamos o poetry (https://python-poetry.org/). Uso é simples e a doc é boa, seguir instalação por lá conforme esta seu ambiente. Alguns comandos úteis:

    $ poetry shell   # abre um shell com virtualenv configurado
    $ poetry install  # instala as deps
    $ poetry add pydantic

E assim por diante.

## Dependências

Além das listadas no `pyproject.toml`, para o celery precisaremos do `redis` instalado localmente.

### Virtualenv

Para ativar o virtualenv com as deps instaladas, use o poetry. Recomendado.

    $ poetry shell
    (.venv) $

A partir desse ponto você tem um ambiente python com as deps instaladas e isoladas, sem conflitos com outros projetos.

## Docker

WIP

## Rodando

### Env vars

Antes, configure seu `.env`. Há um `.env.dist` que serve de exemplo.

### Comandos

Localmente, via poetry shell e python direto. No caso se seu crawler faz uso do celery, primeiro precisa subir os workers. Há uma dependência pelo redis que funciona com o broker e backend para o celery. Via docker-compose não há necessidade do redis instalado local pois lá já sobe um.

    $ celery -A tasks worker -Q crawlers --concurrency=2 -E --loglevel=INFO

Ajuste o loglevel conforme preferir.

Em outro shell é possível disparar uma task via command line. Exemplo:

    $ python commands.py tjba --start-date 2020-05-01 --end-date 2020-05-30

## Produção

WIP