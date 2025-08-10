package com.devscoop.api.consumer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ElasticSearchPostConsumer {

    private final ObjectMapper objectMapper;
    private final ElasticsearchClient esClient;

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    @KafkaListener(topics = "raw-posts", groupId = "raw-posts-es")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            var node = objectMapper.readTree(record.value());

            String id = node.get("url").asText();
            String title = node.get("title").asText();
            String source = node.get("source").asText();

            // createdAt → epoch_millis
            long createdAtMillis = node.has("time")
                    ? node.get("time").asLong() * 1000L
                    : Instant.now().toEpochMilli();

            // keywords
            List<String> keywords = node.has("keywords")
                    ? objectMapper.convertValue(node.get("keywords"),
                    new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {})
                    : null;

            Map<String, Object> document = new HashMap<>();
            document.put("title", title);
            document.put("source", source);
            document.put("createdAt", createdAtMillis);
            document.put("date_kst", LocalDate.now(KST).toString());
            if (keywords != null) document.put("keywords", keywords);

            var request = IndexRequest.of(i -> i
                    .index("raw-posts")
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
        long nowMillis = Instant.now().toEpochMilli();

        for (String raw : keywords) {
            String keyword = raw.toLowerCase();

            try {
                esClient.update(u -> u
                                .index("keyword-stats")
                                .id(keyword)
                                .script(s -> s
                                        .source("""
                                if (ctx._source.total_count == null) ctx._source.total_count = 0;
                                ctx._source.total_count += params.inc;

                                if (ctx._source.sources == null) ctx._source.sources = new HashMap();
                                def s = params.source;
                                ctx._source.sources[s] = (ctx._source.sources.containsKey(s)
                                   ? ctx._source.sources[s] + params.inc
                                   : params.inc);

                                ctx._source.last_updated = params.now;
                                ctx._source.keyword = params.keyword;
                            """)
                                        .lang("painless")
                                        .params("inc",     JsonData.of(1))
                                        .params("source",  JsonData.of(source))
                                        .params("now",     JsonData.of(nowMillis))
                                        .params("keyword", JsonData.of(keyword))
                                )
                                // upsert 문서: 최초 생성 시 값 채움
                                .upsert(Map.of(
                                        "keyword", keyword,
                                        "total_count", 1L,
                                        "sources", Map.of(source, 1L),
                                        "last_updated", nowMillis
                                )),
                        Map.class
                );

                log.info("[ES] Upserted keyword stat: {}", keyword);
            } catch (Exception e) {
                log.error("[ES] Failed to upsert keyword stat: {}", keyword, e);
            }
        }
    }
}
