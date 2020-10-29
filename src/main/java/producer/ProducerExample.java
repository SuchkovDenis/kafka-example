package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Properties;

public class ProducerExample {
  public static void main(String[] args) throws Exception{
    Properties kafkaProps = new Properties();
    kafkaProps.putAll(
        Map.of(
            "bootstrap.servers", "localhost:9092",
            "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
            "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
        )
    );

    var producer = new KafkaProducer<String, String>(kafkaProps);

    // синхронная отправка сообщения
    var record1 = new ProducerRecord<>("CustomerCountry", "Precision Products", "Russia");
    var recordMeta = producer.send(record1).get();
    System.out.println("sync " + recordMeta);

    // асинхронная отправка сообщения и вызов callback
    var record2 = new ProducerRecord<>("CustomerCountry", "Biomedical Materials", "USA");
    producer.send(record2, ProducerExample::onCompletion);

    Thread.sleep(1000);
  }


  public static void onCompletion(RecordMetadata recordMetadata, Exception e) {
    System.out.println("async " +recordMetadata);
  }
}
