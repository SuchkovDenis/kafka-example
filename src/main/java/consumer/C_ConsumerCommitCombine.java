package consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.MILLIS;

public class C_ConsumerCommitCombine {

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
      try {
        while (true) {
          var records = consumer.poll(Duration.of(100, MILLIS));
          for (var record : records) {
            System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
          }
          consumer.commitAsync(); // пользуемся быстрым асинхронным commit
        }
      } catch (Exception e) {
        System.out.printf("Unexpected error %s\n", e);
      } finally {
        consumer.commitSync(); // в случае ошибки воспользуемся commitSync() для гарантированного commit
      }
    }
  }
}
