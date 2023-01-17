<h1 align="center"> Desafio 2RPNET </h1>

Bucket destinado a projeto da 2rpnet

# Projetas
## [LINK DO CÓDIGO NO GOOGLE COLAB - BRONZE_INGESTÃO](https://colab.research.google.com/drive/1iW25EWhOBRdWgHoR2AgQJhK-YamhhHj-?usp=sharing)
## [LINK DO CÓDIGO NO GOOGLE COLAB - SILVER_PRESCRIPTIONS](https://colab.research.google.com/drive/13LQDBY8JTykLk15UxVlV6lQds5MPV4Ko?usp=sharing)
## [LINK DO CÓDIGO NO GOOGLE COLAB - SILVER_PRESCRIBERS](https://colab.research.google.com/drive/1heMs1ZyE3IrFU-8DqSG2x0ZsyRrfUvrH?usp=sharing)
## [LINK DO CÓDIGO NO GOOGLE COLAB - GOLD(DATAFRAMES SOLICITADOS)](https://colab.research.google.com/drive/1iW25EWhOBRdWgHoR2AgQJhK-YamhhHj-?usp=sharing)

Explicando um pouco como o projeto funciona, estou fazendo a ingestão dos dados da API LIMITANDO EM 30MIL LINHAS POR MÊS. Após a ingestão faço um commit dos dados para o repositório criado para o desafio da 2RPNET no github e clono esse mesmo repositório  no ambiente colab dos códigos das camadas seguintes, funcionando praticamente como um storage em nuvem. Conforme solicitado, separei prescriptions de prescribers na camada silver e fiz um DF para cada. Não fiz a ingestão do último mês disponível na api (10/2022) pois foi o que eu entendi da documentação, mas caso seja necessário é so rodar o processo que o mês de Outubro será incluso. O schedule do processo está na pasta Airflow onde está presente um script com a orquestração dos jobs.
As resposta(DataFrames) das questões que foram feitas estão no código da camada  gold, onde usei SQL para retornar os resultados.
  