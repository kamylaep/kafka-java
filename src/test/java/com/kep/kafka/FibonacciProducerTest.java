package com.kep.kafka;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.kep.kafka.FibonacciProducer.TOPIC;

public class FibonacciProducerTest {

  private static final int N = 10;
  private static final List<Long> EXPECTED_VALUES = Arrays.asList(0L, 1L, 1L, 2L, 3L, 5L, 8L, 13L, 21L, 34L);

  @RegisterExtension
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

  @Test
  public void produceFibonacciSequenceWithSuccess() {
    String kafkaConnectString = sharedKafkaTestResource.getKafkaConnectString();
    FibonacciProducer fibonacciProducer = new FibonacciProducer(kafkaConnectString);
    fibonacciProducer.produceFibonacciSequence(N);

    KafkaTestUtils kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();
    List<ConsumerRecord<String, Long>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(TOPIC, StringDeserializer.class, LongDeserializer.class);
    Assertions.assertEquals(N, consumerRecords.size());

    List<Long> actualValues = consumerRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    List<String> actualKeys = consumerRecords.stream().map(ConsumerRecord::key).collect(Collectors.toList());

    Assertions.assertEquals(EXPECTED_VALUES, actualValues);
    Assertions.assertEquals(EXPECTED_VALUES.stream().map(Objects::toString).collect(Collectors.toList()), actualKeys);
  }

}
