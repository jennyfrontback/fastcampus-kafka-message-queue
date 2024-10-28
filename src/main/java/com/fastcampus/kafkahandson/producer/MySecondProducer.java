package com.fastcampus.kafkahandson.producer;

import com.fastcampus.kafkahandson.model.MyMessage;
import com.fastcampus.kafkahandson.model.Topic;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class MySecondProducer {

    private final KafkaTemplate<String, String> secondKafkaTemplate;
    public void sendMessageWithKey(String key, String myMessage){
        secondKafkaTemplate.send(Topic.MY_SECOND_TOPIC, key, myMessage);
    }
}
