# kafka-example
Репозиторий с примерами работы с kafka


## Установка Kafka

```bash
brew install kafka
```

## Запуск zookeeper

```bash
zkServer start
```

## Запуск сервера Kafka

```bash
/usr/local/bin/kafka-server-start /usr/local/etc/kafka/server.properties
```

## Создание топика Kafka

```bash
/usr/local/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
```

## Получить информацию о топике Kafka

```bash
/usr/local/bin/kafka-topics --zookeeper localhost:2181 --describe --topic test
```

## Консольный Kafka producer 

```bash
/usr/local/bin/kafka-console-producer --broker-list localhost:9092 --topic test
```

## Консольный Kafka consumer

```bash
/usr/local/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
```
