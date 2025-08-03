package com.devscoop.api.producer;

import com.devscoop.api.extractor.TechKeywordExtractor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class CrawledDataProducerService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TechKeywordExtractor techKeywordExtractor; // 추가

    public void send(String topic, String key, String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(json);

            String title = node.get("title").asText();
            List<String> keywords = techKeywordExtractor.extractKeywords(title);

            // keywords 필드를 JSON에 추가
            ((ObjectNode) node).putPOJO("keywords", keywords);

            kafkaTemplate.send(topic, key, mapper.writeValueAsString(node));
        } catch (Exception e) {
            log.error("[Producer] Failed to send message to topic={}, key={}", topic, key, e);
        }
    }
}
