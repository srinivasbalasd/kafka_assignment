package com.handson.kafka_assignment.consumer;


import com.handson.kafka_assignment.model.UserDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class KafkaJsonMessageConsumer {

    @Autowired
    private ConsumerFactory<String, UserDto> consumerJsonFactory;


    private static final Logger log = LoggerFactory.getLogger(KafkaJsonMessageConsumer.class);


    @Value("${topic.userdto.name}")
    private String userdtoTopic;

    public List<UserDto> consumeMessages(int partition, long startOffset, long endOffset) {
        List<UserDto> jsonMessages = new ArrayList<>((int) endOffset - (int) startOffset + 1);
        try (KafkaConsumer<String, UserDto> consumer = new KafkaConsumer<>(consumerJsonFactory.getConfigurationProperties())) {
            TopicPartition topicPartition = new TopicPartition(userdtoTopic, partition);
            consumer.assign(Arrays.asList(topicPartition));
            consumer.seek(topicPartition, startOffset);

            ConsumerRecords<String, UserDto> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, UserDto> record : records) {
                if (record.offset() > endOffset) {
                    break;
                }
                log.info("Consumed message: " + record.value() + ", offset: " + record.offset());
                jsonMessages.add(record.value());
            }
            consumer.commitAsync();
        } catch (Exception e) {
            log.error("Error while consuming messages: " + e.getMessage());
            throw e;
        }
        return jsonMessages;
    }


}
