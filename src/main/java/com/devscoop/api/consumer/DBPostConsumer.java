package com.devscoop.api.consumer;

import com.devscoop.api.entity.RawPost;
import com.devscoop.api.repository.KeywordFreqRepository;
import com.devscoop.api.repository.RawPostRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class DBPostConsumer {

    private final RawPostRepository rawPostRepository;
    private final KeywordFreqRepository keywordFreqRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "raw-posts", groupId = "raw-posts-db")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            log.info("[DB] Consumed message partition={}, offset={}", record.partition(), record.offset());

            var node = objectMapper.readTree(record.value());

            // 1. raw_post 저장
            RawPost rawPost = RawPost.builder()
                    .source(node.get("source").asText())
                    .title(node.get("title").asText())
                    .url(node.get("url").asText())
                    .createdAt(toLocalDateTime(node))
                    .build();
            rawPostRepository.save(rawPost);

            // 2. keywords 처리 (producer에서 붙여준 keywords 사용)
            if (node.has("keywords")) {
                List<String> keywords = objectMapper.convertValue(
                        node.get("keywords"),
                        new TypeReference<List<String>>() {}
                );

                for (String keyword : keywords) {
                    keywordFreqRepository.upsert(
                            keyword.toLowerCase(),
                            rawPost.getSource(),
                            LocalDate.now()
                    );
                }
            }

        } catch (Exception e) {
            log.error("[DB] Failed to consume raw-posts", e);
        }
    }

    /**
     * postedAt이 있으면 우선 사용하고, 없으면 time(epoch), 둘 다 없으면 현재 시간
     */
    private LocalDateTime toLocalDateTime(com.fasterxml.jackson.databind.JsonNode node) {
        // postedAt 문자열 우선
        if (node.has("postedAt") && !node.get("postedAt").isNull()) {
            String postedAtStr = node.get("postedAt").asText();
            try {
                return LocalDateTime.parse(postedAtStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            } catch (Exception e) {
                log.warn("[DB] Invalid postedAt format: {}", postedAtStr);
            }
        }

        // time(epoch) fallback
        if (node.has("time") && node.get("time").isNumber()) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(node.get("time").asLong()), ZoneOffset.UTC);
        }

        // default: now
        return LocalDateTime.now();
    }
}
