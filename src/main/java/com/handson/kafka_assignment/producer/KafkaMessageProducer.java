package com.handson.kafka_assignment.producer;



import com.handson.kafka_assignment.model.UserDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaMessageProducer {
    @Value("${topic.string.name}")
    private String topic;

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, UserDto> kafkaJsonTemplate;

    public KafkaMessageProducer(KafkaTemplate<String, String> kafkaTemplate,KafkaTemplate<String, UserDto> kafkaJsonTemplate ) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaJsonTemplate = kafkaJsonTemplate;
    }

    public void sendMessage(String message) {

        kafkaTemplate.send(topic, message);
        log.info("Message : {} sent to topic : {}",message,topic);
    }
}
