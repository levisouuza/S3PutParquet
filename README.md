# S3PutParquet

O [S3PutParquet](https://github.com/levisouuza/S3PutParquet/blob/master/main.py) é um script que realiza um processo de transformação em arquivos .csv para .parquet oriundo de um get de um bucket S3 e re-inserindo-o como parquet.

Arquivos em formato [parquet](https://databricks.com/glossary/what-is-parquet) apresentam dados em estrutura colunar possibilitando otimização nas consultas e redução no custo de armazenamento. As consultas em arquivos parquet oferecem o artifício em que consultas utilizem apenas as colunas desejadas. Além disso, como as colunas apresentam os mesmos datatypes, é possível maximizar a taxa de compreenssão.

As libs principais utilizadas foram pandas, para transformação e boto3, para manipulação no S3 . 

Um ponto de atenção é que a transformação é realizada na memória, logo, para casos de arquivos com tamanho relevante, tal processo seria mais eficiente utilizando o Apache Spark, por exemplo. 

O resultado da execução do script está logo abaixo:

![comparacao](https://github.com/levisouuza/S3PutParquet/blob/master/comparacao_csv_parquet.png)
