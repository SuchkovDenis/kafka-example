package consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.temporal.ChronoUnit.MILLIS;

public class A_ConsumerExample {

  private static Map<String, Integer> custCountryMap = new HashMap<>();

  public static void main(String[] args) throws Exception{
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
          int updatedCount = 1;
          if (custCountryMap.containsKey(record.value())) {
            updatedCount = custCountryMap.get(record.value()) + 1;
          }
          custCountryMap.put(record.value(), updatedCount);
          System.out.println(custCountryMap);
        }
      }
    }
  }
}
