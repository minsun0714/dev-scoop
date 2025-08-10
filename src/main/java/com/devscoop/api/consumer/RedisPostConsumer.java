package com.devscoop.api.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.*;
import java.util.Date;
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
            JsonNode node = objectMapper.readTree(record.value());

            // 0) 필드 추출 & 정규화
            String rawSource = textOrEmpty(node, "source");
            String site = normalizeSource(rawSource); // 소문자 + alias 통일
            String title = textOrEmpty(node, "title");

            // 1) 버킷 날짜 확정 (KST, yyyy-MM-dd)
            String date = resolveDateBucketKst(node); // createdAt/time/date_kst/now 순

            // 2) 키워드 추출/정규화
            List<String> keywords = node.has("keywords")
                    ? objectMapper.convertValue(
                    node.get("keywords"),
                    new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {})
                    : List.of();
            keywords = keywords.stream()
                    .map(s -> s == null ? "" : s.trim().toLowerCase())
                    .filter(s -> !s.isEmpty())
                    .toList();

            if (keywords.isEmpty()) {
                log.info("[Redis] No keywords for site={}, title={}", site, title);
                return;
            }

            // 3) 집계 키 (사이트별 / 전체)
            String redisKey = KEYWORD_PREFIX + site + ":" + date; // ex) keyword_count:hackernews:2025-08-09
            String allKey   = KEYWORD_PREFIX + "all:" + date;

            for (String k : keywords) {
                redisTemplate.opsForZSet().incrementScore(redisKey, k, 1);
                redisTemplate.opsForZSet().incrementScore(allKey,  k, 1);
            }

            // 4) TTL: date(버킷)의 자정 기준 D+3 00:00 KST, 실패 시 2일
            if (!setExpireAtByDate(redisKey, date) | !setExpireAtByDate(allKey, date)) {
                // date 파싱 실패 등일 때 폴백
                redisTemplate.expire(redisKey, Duration.ofDays(2));
                redisTemplate.expire(allKey,  Duration.ofDays(2));
            }

            log.info("[Redis] Updated keyword counts site={}, date={}, title={}", site, date, title);

        } catch (Exception e) {
            log.error("[Redis] Failed to consume raw-posts", e);
        }
    }

    private String resolveDateBucketKst(JsonNode node) {
        // 1) createdAt (epoch millis or ISO)
        if (node.has("createdAt")) {
            var c = node.get("createdAt");
            if (c.canConvertToLong()) {
                return Instant.ofEpochMilli(c.asLong()).atZone(KST).toLocalDate().toString();
            }
            if (c.isTextual()) {
                String s = c.asText();
                try { return Instant.parse(s).atZone(KST).toLocalDate().toString(); } catch (Exception ignore) {}
                try { return LocalDateTime.parse(s).atZone(ZoneOffset.UTC).withZoneSameInstant(KST).toLocalDate().toString(); } catch (Exception ignore) {}
            }
        }
        // 2) time (epoch seconds)
        if (node.has("time") && node.get("time").canConvertToLong()) {
            long ms = node.get("time").asLong() * 1000L;
            return Instant.ofEpochMilli(ms).atZone(KST).toLocalDate().toString();
        }
        // 3) date_kst (YYYY-MM-DD 텍스트) → 그대로 사용
        if (node.has("date_kst") && node.get("date_kst").isTextual()) {
            String d = node.get("date_kst").asText();
            if (d.matches("\\d{4}-\\d{2}-\\d{2}")) return d;
        }
        // 4) 최후 폴백: 수신 시각 기준
        return LocalDate.now(KST).toString();
    }

    private boolean setExpireAtByDate(String key, String yyyyMmDd) {
        try {
            LocalDate d = LocalDate.parse(yyyyMmDd);
            ZonedDateTime expireAtZdt = d.plusDays(3).atStartOfDay(KST); // D+3 00:00
            redisTemplate.expireAt(key, Date.from(expireAtZdt.toInstant()));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String normalizeSource(String s) {
        String x = (s == null ? "" : s.trim().toLowerCase());
        // alias 통일
        if (x.equals("dev.to") || x.equals("dev-to")) x = "devto";
        return x;
    }

    private String textOrEmpty(JsonNode node, String field) {
        return (node.has(field) && node.get(field).isTextual()) ? node.get(field).asText() : "";
    }

}
