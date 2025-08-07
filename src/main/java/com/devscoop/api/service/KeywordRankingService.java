package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import com.devscoop.api.dto.KeywordRankingDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeywordRankingService {

    private static final double ALPHA = 1.0;
    private static final List<String> SOURCES = List.of("github", "hackernews", "reddit");

    private final RedisTemplate<String, String> redisTemplate;
    private final ElasticsearchClient esClient;

    public List<KeywordRankingDto> getKeywordRanking(String source, int limit) {
        Map<String, Integer> todayCounts = fetchKeywordCounts(source, LocalDate.now(), limit);
        Map<String, Integer> yesterdayCounts = fetchKeywordCounts(source, LocalDate.now().minusDays(1), limit);

        return todayCounts.entrySet().stream()
                .map(entry -> {
                    String keyword = entry.getKey();
                    int todayCount = entry.getValue();
                    int yesterdayCount = yesterdayCounts.getOrDefault(keyword, 0);

                    Stat stat = getKeywordStat(source, keyword);
                    double score = calcTrendScore(todayCount, yesterdayCount, stat.mean, stat.std);

                    return KeywordRankingDto.builder()
                            .keyword(keyword)
                            .todayCount(todayCount)
                            .yesterdayCount(yesterdayCount)
                            .score(score)
                            .build();
                })
                .sorted(Comparator.comparingDouble(KeywordRankingDto::score).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    private Map<String, Integer> fetchKeywordCounts(String source, LocalDate date, int limit) {
        String key = "keyword_count:" + source + ":" + date;

        Set<ZSetOperations.TypedTuple<String>> tuples =
                redisTemplate.opsForZSet().reverseRangeWithScores(key, 0, limit * 2L);

        Map<String, Integer> result = new HashMap<>();

        if (tuples != null) {
            for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                String keyword = tuple.getValue();
                Double count = tuple.getScore();
                if (keyword != null && count != null) {
                    result.put(keyword, count.intValue());
                }
            }
        }

        return result;
    }

    private Stat getKeywordStat(String source, String keyword) {
        if (!"all".equals(source)) {
            return getSingleSourceStat(source, keyword);
        }

        List<Stat> stats = SOURCES.stream()
                .map(s -> getSingleSourceStat(s, keyword))
                .filter(Objects::nonNull)
                .filter(stat -> stat.count > 0)
                .toList();

        if (stats.isEmpty()) return new Stat(0.0, 1.0, 1L);

        double totalCount = stats.stream().mapToDouble(s -> s.count).sum();

        double weightedMean = stats.stream()
                .mapToDouble(s -> s.mean * s.count)
                .sum() / totalCount;

        double weightedVariance = stats.stream()
                .mapToDouble(s -> s.count * Math.pow(s.mean - weightedMean, 2))
                .sum() / totalCount;

        double weightedStd = Math.sqrt(weightedVariance);

        return new Stat(weightedMean, weightedStd, (long) totalCount);
    }

    private Stat getSingleSourceStat(String source, String keyword) {
        String redisKey = "keyword_stats:" + source + ":" + keyword;
        List<Object> cached = redisTemplate.opsForHash().multiGet(redisKey, List.of("mean", "std", "count"));

        if (cached.get(0) != null && cached.get(1) != null && cached.get(2) != null) {
            return new Stat(
                    Double.parseDouble(cached.get(0).toString()),
                    Double.parseDouble(cached.get(1).toString()),
                    Long.parseLong(cached.get(2).toString())
            );
        }

        try {
            GetResponse<Map> response = esClient.get(g -> g
                    .index("keyword-stats")
                    .id(keyword), Map.class);

            if (!response.found()) {
                log.warn("[ES] No stats found for '{}:{}'", source, keyword);
                return new Stat(0.0, 1.0, 1L);
            }

            Map<String, Object> doc = response.source();
            if (doc == null) return new Stat(0.0, 1.0, 1L);

            double mean = ((Number) doc.getOrDefault("mean", 0.0)).doubleValue();
            double std = ((Number) doc.getOrDefault("std_dev", 1.0)).doubleValue();

            long count;
            if ("all".equals(source)) {
                count = ((Number) doc.getOrDefault("total_count", 1)).longValue();
            } else {
                Map<String, Object> sources = (Map<String, Object>) doc.get("sources");
                count = (sources != null && sources.get(source) != null)
                        ? ((Number) sources.get(source)).longValue()
                        : 1L;
            }

            redisTemplate.opsForHash().put(redisKey, "mean", String.valueOf(mean));
            redisTemplate.opsForHash().put(redisKey, "std", String.valueOf(std));
            redisTemplate.opsForHash().put(redisKey, "count", String.valueOf(count));
            redisTemplate.expire(redisKey, Duration.ofDays(2));

            return new Stat(mean, std, count);

        } catch (Exception e) {
            log.error("[ES] Failed to fetch stat for '{}:{}'", source, keyword, e);
            return new Stat(0.0, 1.0, 1L);
        }
    }

    private double calcTrendScore(int todayCount, int yesterdayCount, double mean, double std) {
        double growth = (double) (todayCount - yesterdayCount) / (ALPHA + todayCount);
        double standardized = (std == 0) ? 1.0 : (todayCount - mean) / std;
        return growth * standardized;
    }

    private record Stat(double mean, double std, long count) {}
}
