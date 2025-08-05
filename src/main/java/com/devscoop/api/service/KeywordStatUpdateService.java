package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class KeywordStatUpdateService {

    private final ElasticsearchClient esClient;
    private final StringRedisTemplate redisTemplate;

    // 사용할 데이터 소스 목록
    private static final List<String> SOURCES = List.of("github", "hn", "reddit");

    public void updateMeanAndStd(String keyword) {
        try {
            List<Integer> allCounts = new ArrayList<>();

            for (String source : SOURCES) {
                String redisKey = String.format("keyword_stats:%s:%s", source, keyword);
                Set<String> members = redisTemplate.opsForZSet().range(redisKey, 0, -1);

                if (members != null) {
                    for (String member : members) {
                        Double score = redisTemplate.opsForZSet().score(redisKey, member);
                        if (score != null) {
                            allCounts.add(score.intValue());
                        }
                    }
                }
            }

            if (allCounts.isEmpty()) {
                log.warn("[Redis] No stats found for '{}', skipping update", keyword);
                return;
            }

            double mean = calculateMean(allCounts);
            double stdDev = calculateStdDev(allCounts, mean);

            Map<String, Object> doc = Map.of(
                    "mean", mean,
                    "std_dev", stdDev,
                    "last_updated", Instant.now().toString()
            );

            UpdateRequest<Map<String, Object>, Map<String, Object>> request = UpdateRequest.of(u -> u
                    .index("keyword-stats")
                    .id(keyword)
                    .doc(doc)
                    .docAsUpsert(true)
            );

            esClient.update(request, Map.class);
            log.info("[ES] Updated mean/std for '{}': mean={}, std={}", keyword, mean, stdDev);

        } catch (Exception e) {
            log.error("[ES] Failed to update mean/std for '{}'", keyword, e);
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
