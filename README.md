# Projeto Apache Cassandra - Pipeline de Dados

## Descrição
Pipeline de dados em tempo real integrando Apache Kafka, Apache Spark Streaming e Apache Cassandra.
Os dados são publicados em um tópico Kafka, processados pelo Spark e persistidos no Cassandra.

## Tecnologias
- Apache Cassandra 4.1.11
- Apache Kafka 3.8.0
- Apache Spark 3.5.1 (PySpark)
- Python 3.x
- Java 11

## Estrutura
- pipeline.py — script do Spark Streaming que consome do Kafka e salva no Cassandra

## Como Executar

1. Subir o Cassandra: sudo cassandra -R -f

2. Subir o Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties

3. Subir o Kafka: bin/kafka-server-start.sh config/server.properties

4. Criar o tópico: bin/kafka-topics.sh --create --topic topico_vendas --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

5. Rodar o pipeline: spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pipeline.py

6. Enviar dados pelo producer do Kafka em formato JSON

7. Verificar no cqlsh: SELECT * FROM minha_loja.vendas;
