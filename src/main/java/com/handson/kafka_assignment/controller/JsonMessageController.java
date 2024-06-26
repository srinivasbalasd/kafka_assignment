package com.handson.kafka_assignment.controller;


import com.handson.kafka_assignment.consumer.KafkaJsonMessageConsumer;
import com.handson.kafka_assignment.model.UserDto;
import com.handson.kafka_assignment.producer.KafkaJsonMessageProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class JsonMessageController {



    private final KafkaJsonMessageProducer kafkaJsonMessageProducer;
    private final KafkaJsonMessageConsumer kafkaJsonMessageConsumer;

    public JsonMessageController(KafkaJsonMessageProducer kafkaJsonMessageProducer, KafkaJsonMessageConsumer kafkaJsonMessageConsumer) {
        this.kafkaJsonMessageProducer = kafkaJsonMessageProducer;
        this.kafkaJsonMessageConsumer = kafkaJsonMessageConsumer;
    }

    @PostMapping("/sendJson")
    public String sendMessage( @RequestBody UserDto user) {
        kafkaJsonMessageProducer.sendJsonMessage(user);
        return "Message sent: " + user;
    }



    @PostMapping("/setJsonOffsets")
    public ResponseEntity setJsonOffsets(@RequestParam int partition, @RequestParam long startOffset, @RequestParam long endOffset) {
        try {
            List<UserDto> messages = kafkaJsonMessageConsumer.consumeMessages(partition, startOffset, endOffset);
            return ResponseEntity.ok("Messages consumed from offset " + startOffset + " to " + endOffset + " in partition : " + partition + ", Messages : " + messages);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
}
