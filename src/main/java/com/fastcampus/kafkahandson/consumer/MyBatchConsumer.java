package com.fastcampus.kafkahandson.consumer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fastcampus.kafkahandson.model.Topic;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.DataInput;
import java.util.List;

@Component
public class MyBatchConsumer {

    MyBatchConsumer(){
        System.out.println("MyConsumer init!");
    }

    @KafkaListener(
            topics = {Topic.MY_JSON_TOPIC},
            groupId = "batch-test-consumer-group",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void accept(List<ConsumerRecord<String, String>> messages){
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println("[Batch Consumer] => Messages arrived! - "+ messages.size());
        messages.forEach(message -> {
            MyMessage myMessage = null;
            try {
                myMessage = objectMapper.readValue(message.value(), MyMessage.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            System.out.println(">>>>> message : " + myMessage + "/ offset: "+ message.offset() + "/ partition : " + message.partition());
        });
    }
}