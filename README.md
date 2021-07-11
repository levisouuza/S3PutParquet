# S3PutParquet

O [S3PutParquet](https://github.com/levisouuza/S3PutParquet/blob/master/main.py) é um script que realiza um processo de transformação em arquivos .csv para .parquet oriundo de um get de um bucket S3 e re-inserindo-o como parquet.

As libs principais utilizadas foram pandas, para transformação e boto3, para manipulação no S3 . 

Um ponto de atenção é que a transformação é realizada na memória, logo, para casos de arquivos com tamanho relevante, tal processo seria mais eficiente utilizando o Apache Spark, por exemplo. 
