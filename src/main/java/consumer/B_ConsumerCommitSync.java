package consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.MILLIS;

public class B_ConsumerCommitSync {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.putAll(
        Map.of(
            "bootstrap.servers", "localhost:9092",
            "group.id", "CountryCounter",
            "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"
        )
    );

    try (var consumer = new KafkaConsumer<String, String>(props)) {
      consumer.subscribe(List.of("CustomerCountry"));
      while (true) {
        var records = consumer.poll(Duration.of(100, MILLIS));
        for (var record : records) {
          System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
        try {
          consumer.commitSync(); // блокирующий синхронный commit. Уменьшает пропускную спрсобность
          // consumer.commitAsync(); // неблокирующий асинхронный commit. Могут быть дублирующие сообщения
        } catch (CommitFailedException e) {
          System.out.printf("commit failed %s\n", e);
        }
      }
    }
  }
}
