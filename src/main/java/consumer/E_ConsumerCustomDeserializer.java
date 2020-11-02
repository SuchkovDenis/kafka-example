package consumer;

import consumer.serializer.Customer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.MILLIS;

public class E_ConsumerCustomDeserializer {

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

    try (var consumer = new KafkaConsumer<String, Customer>(props)) {
      consumer.subscribe(List.of("CustomerCountry"));
      while (true) {
        var records = consumer.poll(Duration.of(100, MILLIS));
        for (var record : records) {
          System.out.println("current customer Id: " + record.value().getID() + " and current customer name: " + record.value().getName());
        }
      }
    }
  }
}
