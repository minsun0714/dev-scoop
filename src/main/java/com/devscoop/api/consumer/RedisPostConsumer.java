package com.devscoop.api.consumer;

import com.devscoop.api.extractor.TechKeywordExtractor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisPostConsumer {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String KEYWORD_PREFIX = "keyword_count:";
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    @KafkaListener(topics = "raw-posts", groupId = "raw-posts-redis")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            var node = objectMapper.readTree(record.value());

            String site = node.get("source").asText();
            String title = node.get("title").asText();
            String date = LocalDate.now(KST).toString();

            // keywords 필드 우선 사용
            List<String> keywords;
            if (node.has("keywords")) {
                keywords = objectMapper.convertValue(
                        node.get("keywords"),
                        new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {}
                );
            } else {
                log.info("[Redis] No tech keywords found for site={}, title={}", site, title);
                return;
            }

            String redisKey = KEYWORD_PREFIX + site + ":" + date;
            String allKey = KEYWORD_PREFIX + "all:" + date;

            for (String keyword : keywords) {
                redisTemplate.opsForZSet().incrementScore(redisKey, keyword.toLowerCase(), 1);
                redisTemplate.opsForZSet().incrementScore(allKey, keyword.toLowerCase(), 1);
            }

            redisTemplate.expire(redisKey, Duration.ofDays(2));
            redisTemplate.expire(allKey, Duration.ofDays(2));

            log.info("[Redis] Updated keyword counts for site={}, title={}, keywords={}", site, title, keywords);

        } catch (Exception e) {
            log.error("[Redis] Failed to consume raw-posts", e);
        }
    }
}
