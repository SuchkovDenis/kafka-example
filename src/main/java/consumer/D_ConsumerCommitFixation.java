package consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.MILLIS;

public class D_ConsumerCommitFixation {

  private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

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

    int count = 0;
    try (var consumer = new KafkaConsumer<String, String>(props)) {
      consumer.subscribe(List.of("CustomerCountry"));
      while (true) {
        var records = consumer.poll(Duration.of(100, MILLIS));
        for (var record : records) {
          System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());

          // самостоятельно следим за смещением
          currentOffsets.put(
              new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset()+1, "no metadata")
          );

          if (count % 1000 == 0) consumer.commitAsync(currentOffsets, null); // фиксируем каждые 1000 записей
          count++;
        }
      }
    }
  }
}
