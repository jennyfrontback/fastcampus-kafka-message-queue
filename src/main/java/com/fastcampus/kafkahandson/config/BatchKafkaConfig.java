package com.fastcampus.kafkahandson.config;

import com.fastcampus.kafkahandson.model.MyMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class BatchKafkaConfig {

    @Bean
    @Qualifier("batchConsumerFactory")
    public ConsumerFactory<String, String> batchConsumerFactory(@Qualifier("secondKafkaProperties") KafkaProperties kafkaProperties){
        Map<String, Object> props = new HashMap<>();
        System.out.println("key deserializer: " +  kafkaProperties.getConsumer().getKeyDeserializer());
        System.out.println("value deserializer: " +  kafkaProperties.getConsumer().getValueDeserializer());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getConsumer().getKeyDeserializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaProperties.getConsumer().getValueDeserializer());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConsumerConfig.DEFAULT_MAX_POLL_RECORDS);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    @Qualifier("batchKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory(
            @Qualifier("batchConsumerFactory") ConsumerFactory<String, String> batchConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(batchConsumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(1);
        return factory;
    }
}
