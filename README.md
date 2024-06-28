Como utilizar este algoritmo:

Note que este algoritmo utiliza o serviço Google Cloud Vision AI, que cobra um valor por página extraída. Consulta a documentação do Google para verificar a precificação.

(Caso esteja usando Windows, execute-o em ambiente WSL2)

- Aplicações pré-requisitas: docker, poetry, CLI gcloud 
- Também é necessária uma conta da Google Cloud

- Execute 'poetry install' para instalar as dependências do módulo.
- Crie um repositório no Google Cloud Storage e insira o nome deste repositório na variável "BUCKET_NAME" em config.py
- Crie um arquivo nomeado ".env" na raiz desta pasta.
- Insira a sua chave de acesso da Google Vision API no arquivo .env (https://cloud.google.com/vision/product-search/docs/auth?hl=pt-br)
- Faça a autenticação na CLI gcloud ('gcloud auth login && gcloud auth application-default login') - também é necessário definir o projeto de cota com auth application-default set-quota-project <nome-do-projeto>
- Insira o arquivo em PDF desejado na pasta 01_original_files
- execute main.py ('poetry shell' seguido de 'python main.py')

- Caso haja erros na detecção de linhas, altere o espaçamento entre linhas esperado em 'config.py' alterando a variável GAP_BETWEEN_LINES

- As tabelas resultados serão geradas em formato .csv na pasta '06_output_table_files'
- As outras pastas contém etapas intermediárias do processamento e podem ser consultadas para correção de erros
