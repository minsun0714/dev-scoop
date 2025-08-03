package com.devscoop.api.consumer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ElasticSearchPostConsumer {

    private final ObjectMapper objectMapper;
    private final ElasticsearchClient esClient;

    @KafkaListener(topics = "raw-posts", groupId = "raw-posts-es")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            var node = objectMapper.readTree(record.value());

            String id = node.get("url").asText(); // url을 문서 id로 사용
            String title = node.get("title").asText();
            String source = node.get("source").asText();
            String createdAt = node.has("time")
                    ? Instant.ofEpochSecond(node.get("time").asLong()).toString()
                    : Instant.now().toString();

            // keywords 처리 (producer에서 넣어준 값 사용)
            List<String> keywords = null;
            if (node.has("keywords")) {
                keywords = objectMapper.convertValue(
                        node.get("keywords"),
                        new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {}
                );
            }

            Map<String, Object> document = new HashMap<>();
            document.put("title", title);
            document.put("source", source);
            document.put("createdAt", createdAt);
            if (keywords != null) {
                document.put("keywords", keywords);
            }

            // Elasticsearch 색인
            IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
                    .index("raw-posts") // 인덱스 이름
                    .id(id)
                    .document(document)
            );

            esClient.index(request);

            log.info("[ES] Indexed raw post: {}", title);

        } catch (Exception e) {
            log.error("[ES] Failed to consume raw-posts", e);
        }
    }
}
