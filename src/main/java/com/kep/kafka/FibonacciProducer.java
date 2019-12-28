package com.kep.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

public class FibonacciProducer {

  public static final String KAFKA_BOOTSTRAP_SERVER_DEFAULT = "127.0.0.1:9092";
  public static final String TOPIC = "fibonacci";

  private Logger logger = LoggerFactory.getLogger(FibonacciProducer.class);
  private KafkaProducer<String, Long> producer;

  public FibonacciProducer() {
    this(KAFKA_BOOTSTRAP_SERVER_DEFAULT);
  }

  public FibonacciProducer(String kafkaBootstrapServer) {
    Properties properties = buildProperties(kafkaBootstrapServer);
    producer = new KafkaProducer<>(properties);
  }

  public void produceFibonacciSequence(int n) {
    long first = 0;
    long second = 1;

    for (int i = 0; i < n; i++) {
      ProducerRecord<String, Long> record = new ProducerRecord<>(TOPIC, first + "", first);
      producer.send(record, this::onCompletionCallback);
      producer.flush();

      long sum = first + second;
      first = second;
      second = sum;
    }
  }

  private void onCompletionCallback(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      logger.error("Error producing fibonacci sequence", exception);
      return;
    }

    logger.info("topic={}, partition={}, offset={}", metadata.topic(), metadata.partition(), metadata.offset());
  }

  private Properties buildProperties(String bootstrapServer) {
    Properties properties = new Properties();
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
    return properties;
  }
}
