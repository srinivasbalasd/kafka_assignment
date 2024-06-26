package com.handson.kafka_assignment.controller;


import com.handson.kafka_assignment.consumer.KafkaMessageConsumer;
import com.handson.kafka_assignment.producer.KafkaMessageProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class MessageController {

    private final KafkaMessageProducer messageProducer;
    private final KafkaMessageConsumer kafkaMessageConsumer;

    public MessageController(KafkaMessageProducer messageProducer, KafkaMessageConsumer kafkaMessageConsumer) {
        this.messageProducer = messageProducer;
        this.kafkaMessageConsumer = kafkaMessageConsumer;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        messageProducer.sendMessage(message);
        return "Message sent: " + message;
    }

    @PostMapping("/setOffsets")
    public ResponseEntity setOffsets(@RequestParam int partition, @RequestParam long startOffset, @RequestParam long endOffset) {
        try {
            List<String> messages = kafkaMessageConsumer.consumeMessages(partition, startOffset, endOffset);
            return ResponseEntity.ok("Messages consumed from offset " + startOffset + " to " + endOffset + " in partition : " + partition + ", Messages : " + messages);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
}
