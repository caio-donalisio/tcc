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

    $ python commands.py tjba --start-date 2020-05-01 --end-date 2020-05-30 --output-uri ./data/tjba

Esta é a lista de parâmetros disponíveis para cada tribunal.
Idealmente no futuro todos os crawlers devem ter os mesmos parâmetros.
|  | *start-date* | *end-date* | *start-year* | *end-year* | *output-uri* | *pdf-async* | *skip-pdf* | *skip-cache* | *enqueue* |  *split-tasks* | *browser* | *count-only* |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **CARF** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **SCRFB** | :avocado: | :avocado: |  |  | :avocado: |  |  | | :avocado:  |  :avocado: |  | :avocado: |
| **STF** | :avocado: | :avocado: |  |  | :avocado: | :avocado: | :avocado: |  |  :avocado: |  :avocado: |  |  |
| **STJ** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TITSP** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TJBA** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TJMG** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TJPR** | :avocado: | :avocado: |  |  | :avocado: |  |  | | :avocado:  |  :avocado: |  | :avocado: |
| **TJRJ** | |  | :avocado: | :avocado: | :avocado: | :avocado: | :avocado: |  |  :avocado: |  :avocado: |  |  |
| **TJRS** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TJSP** | :avocado: | :avocado: |  |  | :avocado: | :avocado: | :avocado: | :avocado: | :avocado: | :avocado: | :avocado: |  |
| **TRF1** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TRF2** | :avocado: | :avocado: |  |  | :avocado: | :avocado: | :avocado: |  |  :avocado: |  :avocado: |  |  |
| **TRF3** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TRF4** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TRF5** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |
| **TST** | :avocado: | :avocado: |  |  | :avocado: |  |  |  |  :avocado: |  :avocado: |  |  |

<br>

### Parâmetros:
<br>

 
 &emsp;**start-date:**  &emsp;Data **inicial** da coleta dos processos, pode ser de julgamento ou de publicação, dependendo do tribunal (e.g. 2022-12-31).

 &emsp;**end-date:**  &emsp;Data **final** da coleta dos processos, pode ser de julgamento ou de publicação, dependendo do tribunal (e.g. 2022-12-31).

 &emsp;**start-year:**  &emsp;Ano **inicial** da coleta dos processos, pode ser de julgamento ou de publicação, dependendo do tribunal (e.g. 2019).

 &emsp;**end-year:**  &emsp;Ano **final** da coleta dos processos, pode ser de julgamento ou de publicação, dependendo do tribunal (e.g. 2019).

 &emsp;**output-uri:**  &emsp;Diretório onde as pastas (com caminho /{ano}/{mês}/{arquivo} )com os arquivos serão salvos (e.g. gs://inspira-carf ou ./data/trf3 )

 &emsp;**pdf-async:** &emsp; Baixa os PDFs asincronamente (flag)

 &emsp;**skip-pdf:**  &emsp;Ignora a coleta dos pdfs (flag)

 &emsp;**skip-cache:**  &emsp;Ignora os arquivos de progresso e inicia a coleta do zero (flag)

 &emsp;**enqueue:**  &emsp;Torna a coleta em uma task do celery (flag)

 &emsp;**browser:** &emsp; Mostra o browser no processo de coleta (flag)

 &emsp;**split-tasks:**  &emsp;Para ser usado junto ao *enqueue*, divide as tasks em sub-períodos (e.g. --split-tasks months)

 &emsp;**count-only:**  &emsp;Crawler irá apenas coletar as contagens de processos do dado período (flag)

 <br>
 <hr>
<br>
  
&emsp;Para o TJSP, é possível coletar apenas os PDFs dos metadados correspondentes que não tenham um PDF ainda, definindo um ano e mês específico com o parâmetro "prefix":

    $ python commands.py tjsp-pdf --input-uri gs://inspira-production-buckets-tjsp --prefix 2022/04 
<br>

## Produção

WIP
