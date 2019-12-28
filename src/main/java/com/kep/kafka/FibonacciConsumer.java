package com.kep.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class FibonacciConsumer {

  public static final String GROUP_ID = "fibonacci-default-consumer";

  private KafkaConsumer<String, Long> consumer;

  public FibonacciConsumer() {
    this(FibonacciProducer.KAFKA_BOOTSTRAP_SERVER_DEFAULT);
  }

  public FibonacciConsumer(String kafkaBootstrapServer) {
    Properties properties = buildProperties(kafkaBootstrapServer);
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton(FibonacciProducer.TOPIC));
  }

  public void consumeFibonacciSequence(Consumer<ConsumerRecord<String, Long>> callback) {
    while (true) {
      consumer.poll(Duration.ofMillis(100)).forEach(rec -> callback.accept(rec));
    }
  }

  private Properties buildProperties(String bootstrapServer) {
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
    properties.setProperty(GROUP_ID_CONFIG, GROUP_ID);
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }
}
