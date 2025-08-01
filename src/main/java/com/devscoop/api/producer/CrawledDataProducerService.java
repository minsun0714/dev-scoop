package com.devscoop.api.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CrawledDataProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(String topic, String key, String json) {
        kafkaTemplate.send(topic, key, json);
    }
}
