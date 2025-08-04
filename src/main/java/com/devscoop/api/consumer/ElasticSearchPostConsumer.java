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

            if (keywords != null && !keywords.isEmpty()) {
                updateKeywordStats(keywords, source);
            }

            esClient.index(request);

            log.info("[ES] Indexed raw post: {}", title);

        } catch (Exception e) {
            log.error("[ES] Failed to consume raw-posts", e);
        }
    }

    private void updateKeywordStats(List<String> keywords, String source) {
        for (String keyword : keywords) {
            try {
                // 1. 기존 문서 조회
                var response = esClient.get(g -> g
                        .index("keyword-stats")
                        .id(keyword), Map.class);

                Map<String, Object> doc = response.found() ? response.source() : new HashMap<>();

                // 2. total_count 갱신
                int totalCount = (int) doc.getOrDefault("total_count", 0);
                doc.put("total_count", totalCount + 1);

                // 3. sources.{source} 갱신
                Map<String, Integer> sources = (Map<String, Integer>) doc.getOrDefault("sources", new HashMap<>());
                sources.put(source, sources.getOrDefault(source, 0) + 1);
                doc.put("sources", sources);

                // 4. last_updated 갱신
                doc.put("last_updated", Instant.now().toString());

                // 5. keyword와 _id 보장
                doc.put("keyword", keyword);

                // 6. 색인 (없으면 upsert처럼 동작)
                esClient.index(i -> i
                        .index("keyword-stats")
                        .id(keyword)
                        .document(doc));

                log.info("[ES] Updated keyword stat: {}", keyword);
            } catch (Exception e) {
                log.error("[ES] Failed to update keyword stat: {}", keyword, e);
            }
        }
    }
}
