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
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisPostConsumer {

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final TechKeywordExtractor techKeywordExtractor;

    private static final String KEYWORD_PREFIX = "keyword_count:";

    @KafkaListener(topics = "raw-posts", groupId = "raw-posts-redis")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            var node = objectMapper.readTree(record.value());

            String site = node.get("source").asText();
            String title = node.get("title").asText();
            String date = LocalDate.now().toString();

            // OpenAI를 이용해 키워드 추출
            List<String> keywords = techKeywordExtractor.extractKeywords(title);
            if (keywords.isEmpty()) {
                log.info("[Redis] No tech keywords found for site={}, title={}", site, title);
                return;
            }

            String redisKey = KEYWORD_PREFIX + site + ":" + date;
            String allKey = KEYWORD_PREFIX + "all:" + date;

            for (String keyword : keywords) {
                redisTemplate.opsForZSet().incrementScore(redisKey, keyword.toLowerCase(), 1);
                redisTemplate.opsForZSet().incrementScore(allKey, keyword.toLowerCase(), 1);
            }

            redisTemplate.expire(redisKey, Duration.ofHours(48));
            redisTemplate.expire(allKey, Duration.ofHours(48));

            log.info("[Redis] Updated keyword counts for site={}, title={}, keywords={}", site, title, keywords);

        } catch (Exception e) {
            log.error("[Redis] Failed to consume raw-posts", e);
        }
    }
}
