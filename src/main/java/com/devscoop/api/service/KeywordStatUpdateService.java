package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class KeywordStatUpdateService {

    private final ElasticsearchClient esClient;
    private final StringRedisTemplate redisTemplate;

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final String KEYWORD_PREFIX = "keyword_count:"; // keyword_count:{source}:{yyyy-MM-dd}
    private static final List<String> SOURCES = List.of("github", "hackernews", "reddit");
    private static final int WINDOW_DAYS = 7;

    /** 최근 7일(keyword별, 소스 합계)의 mean/std_dev을 keyword-stats에 upsert */
    public void updateMeanAndStd(String rawKeyword) {
        try {
            String keyword = (rawKeyword == null ? "" : rawKeyword.trim().toLowerCase());
            if (keyword.isEmpty()) {
                log.warn("[Stat] empty keyword");
                return;
            }

            LocalDate today = LocalDate.now(KST);
            List<Integer> dailyTotals = new ArrayList<>(WINDOW_DAYS);

            // 날짜별 합계(모든 소스 합산). 0도 포함해서 '없던 날'을 반영.
            for (int i = WINDOW_DAYS - 1; i >= 0; i--) {
                LocalDate d = today.minusDays(i);
                int sum = 0;
                for (String source : SOURCES) {
                    String redisKey = KEYWORD_PREFIX + source + ":" + d; // e.g. keyword_count:hackernews:2025-08-09
                    Double score = redisTemplate.opsForZSet().score(redisKey, keyword);
                    if (score != null) sum += score.intValue();
                }
                dailyTotals.add(sum);
            }

            double mean = calculateMean(dailyTotals);
            double stdDev = calculateStdDev(dailyTotals, mean);

            Map<String, Object> doc = new HashMap<>();
            doc.put("mean_7d", mean);
            doc.put("std_dev_7d", stdDev);
            doc.put("window_days", WINDOW_DAYS);
            doc.put("last_updated", Instant.now().toEpochMilli());

            UpdateRequest<Map<String, Object>, Map<String, Object>> request = UpdateRequest.of(u -> u
                    .index("keyword-stats")
                    .id(keyword)
                    .doc(doc)
                    .docAsUpsert(true)
            );

            esClient.update(request, Map.class);
            log.info("[ES] Updated 7d mean/std for '{}': mean={}, std={}", keyword, mean, stdDev);

        } catch (Exception e) {
            log.error("[ES] Failed to update mean/std for '{}'", rawKeyword, e);
        }
    }

    private double calculateMean(List<Integer> values) {
        return values.stream().mapToInt(i -> i).average().orElse(0.0);
    }

    private double calculateStdDev(List<Integer> values, double mean) {
        double variance = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average()
                .orElse(0.0);
        return Math.sqrt(variance);
    }
}
