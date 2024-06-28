Este script é utilizado para extrair tabelas de pdfs em imagem

Como utilizar este algoritmo:

Note que este algoritmo utiliza o serviço Google Cloud Vision AI, que cobra um valor por página extraída. Consulta a documentação do Google para verificar a precificação.

(Caso esteja usando Windows, execute-o em ambiente WSL2)

- Aplicações pré-requisitas: docker, poetry, CLI gcloud 
- Também é necessária uma conta da Google Cloud

- Execute 'poetry install' para instalar as dependências do módulo.
- Crie um repositório no Google Cloud Storage e insira o nome deste repositório na variável "BUCKET_NAME" em config.py
- Crie um arquivo nomeado ".env" na raiz desta pasta e insira a linha: GOOGLE_VISION_API_KEY="\<chave de acesso\>" ( para obter a sua chave de acesso, consulte https://cloud.google.com/vision/product-search/docs/auth?hl=pt-br)
- Faça a autenticação na CLI gcloud ("gcloud auth login && gcloud auth application-default login") - também é necessário definir o projeto de cota com "auth application-default set-quota-project <nome-do-projeto>" https://cloud.google.com/docs/quotas/set-quota-project?hl=pt-br
- Insira o arquivo em PDF desejado na pasta 01_original_files
- execute main.py (com os comandos "poetry shell" seguido de "python main.py")

- Caso haja erros na detecção de linhas, altere o espaçamento entre linhas esperado em 'config.py' alterando a variável GAP_BETWEEN_LINES

- As tabelas resultados serão geradas em formato .csv na pasta '06_output_table_files'
- As outras pastas contém etapas intermediárias do processamento e podem ser consultadas para correção de erros

Exemplo de tabela detectada
![QUADRO_0033](https://github.com/caio-donalisio/tcc/assets/58789818/de6a0ce0-d660-48a1-b420-3226de5f4c4a)

Exemplo de tabela exportada
![image](https://github.com/caio-donalisio/tcc/assets/58789818/63fb0786-365d-4a65-8433-3208f038cb1e)
