package com.druid.kafka.kafkaspringdemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class DruidKafkaConsumer {

    @KafkaListener(topics = {"test"})
    public void listener(ConsumerRecord record){

        Optional<?> msg=Optional.ofNullable(record.value());
        if(msg.isPresent()){
            System.out.println(msg.get());
        }
    }
}
