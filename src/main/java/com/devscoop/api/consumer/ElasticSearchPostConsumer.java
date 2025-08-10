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

            String url    = node.path("url").asText("");
            String title  = node.path("title").asText("");
            String source = node.path("source").asText("");

            if (title.isEmpty() || source.isEmpty()) {
                log.warn("[ES] skip: missing required fields source/title. raw={}", record.value());
                return;
            }

            long createdAtMillis = extractCreatedAtMillis(node);
            String dateKst = node.has("date_kst")
                    ? node.path("date_kst").asText()
                    : LocalDate.now(KST).toString();

            List<String> keywords = node.has("keywords") && node.get("keywords").isArray()
                    ? objectMapper.convertValue(node.get("keywords"),
                    new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {})
                    : List.of();

            Map<String, Object> document = new HashMap<>();
            document.put("title", title);
            document.put("source", source);
            document.put("url", url);
            document.put("createdAt", createdAtMillis); // ES date 매핑과 호환(epoch_millis)
            document.put("date_kst", dateKst);
            if (!keywords.isEmpty()) document.put("keywords", keywords);

            String docId = buildDocId(source, url, title); // 안정적/짧은 ID

            var request = IndexRequest.of(i -> i
                    .index("raw-posts")
                    .id(docId)
                    .document(document)
            );

            esClient.index(request); // 1) 먼저 인덱싱
            if (!keywords.isEmpty()) {
                updateKeywordStats(keywords, source); // 2) 그 다음 통계
            }

            log.info("[ES] Indexed raw post: {} ({})", title, docId);

        } catch (Exception e) {
            log.error("[ES] Failed to consume raw-posts", e);
            // TODO: DLQ(보류 큐)로 보내거나 재시도 전략 고려
        }
    }

    private long extractCreatedAtMillis(com.fasterxml.jackson.databind.JsonNode node) {
        if (node.has("time") && node.get("time").canConvertToLong()) {
            return node.get("time").asLong() * 1000L; // HN epoch sec
        }
        if (node.has("createdAt")) {
            var c = node.get("createdAt");
            if (c.canConvertToLong()) return c.asLong(); // epoch_millis
            if (c.isTextual()) {
                String s = c.asText();
                try { return Instant.parse(s).toEpochMilli(); } catch (Exception ignore) {}
            }
        }
        for (String f : new String[]{"postedAt","publishedAt"}) {
            if (node.has(f) && node.get(f).isTextual()) {
                try { return Instant.parse(node.get(f).asText()).toEpochMilli(); } catch (Exception ignore) {}
            }
        }
        return Instant.now().toEpochMilli();
    }

    private String buildDocId(String source, String url, String title) {
        String basis = (url != null && !url.isBlank()) ? normalize(url) : (source + "|" + title);
        return sha256Hex(basis).substring(0, 32); // 32자 고정 (충분히 유니크)
    }

    private String normalize(String s) {
        String t = s == null ? "" : s.trim();
        int i = t.indexOf('?'); if (i > 0) t = t.substring(0, i);
        if (t.endsWith("/")) t = t.substring(0, t.length()-1);
        return t;
    }

    private String sha256Hex(String s) {
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(h.length * 2);
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(s.hashCode()); // fallback
        }
    }

    private void updateKeywordStats(List<String> keywords, String source) {
        long nowMillis = Instant.now().toEpochMilli();

        for (String raw : keywords) {
            String keyword = (raw == null ? "" : raw.trim().toLowerCase());
            if (keyword.isEmpty()) continue;

            try {
                esClient.update(u -> u
                                .index("keyword-stats")
                                .id(keyword)
                                .retryOnConflict(3)
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
