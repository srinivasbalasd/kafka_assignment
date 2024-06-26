package com.handson.kafka_assignment.producer;


import com.handson.kafka_assignment.model.UserDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaJsonMessageProducer {

    @Value("${topic.userdto.name}")
    private String topic;

    private static final Logger log = LoggerFactory.getLogger(KafkaJsonMessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, UserDto> kafkaJsonTemplate;

    public KafkaJsonMessageProducer(KafkaTemplate<String, String> kafkaTemplate, KafkaTemplate<String, UserDto> kafkaJsonTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaJsonTemplate = kafkaJsonTemplate;
    }

    public void sendJsonMessage(UserDto userDto) {

        kafkaJsonTemplate.send(topic, userDto);
        log.info("Message : {} sent to topic : {}", userDto, topic);
    }
}
