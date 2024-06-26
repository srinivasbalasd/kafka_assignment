package com.handson.kafka_assignment.consumer;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Component
@Slf4j
public class KafkaMessageConsumer {

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;


    private static final Logger log = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    /*@KafkaListener(topics = "users", groupId = "my-group-id")
    public void listen(String message) {
        log.info("Received message: " + message);
    }*/

    @Value("${topic.name}")
    private String topic;

    public List<String> consumeMessages(int partition, long startOffset, long endOffset) {
        List<String> messages = new ArrayList<>((int) endOffset - (int) startOffset +1);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerFactory.getConfigurationProperties())) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seek(topicPartition, startOffset);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                if (record.offset() > endOffset) {
                    break;
                }
                log.info("Consumed message: " + record.value() + ", offset: " + record.offset());
                messages.add(record.value());
            }
            consumer.commitAsync();
            consumer.close();
        } catch (Exception e) {
            // Log and handle exception
            log.error("Error while consuming messages: " + e.getMessage());
            throw e;
        }
        return messages;
    }



}
