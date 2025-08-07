package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
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

        Set<String> allKeywords = new HashSet<>(todayCounts.keySet());
        allKeywords.addAll(yesterdayCounts.keySet());

        Map<String, Stat> statMap = fetchKeywordStatsBulk(source, allKeywords);

        return todayCounts.entrySet().stream()
                .map(entry -> {
                    String keyword = entry.getKey();
                    int todayCount = entry.getValue();
                    int yesterdayCount = yesterdayCounts.getOrDefault(keyword, 0);

                    Stat stat = statMap.getOrDefault(keyword, new Stat(0.0, 1.0, 1L));
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

    private Map<String, Stat> fetchKeywordStatsBulk(String source, Set<String> keywords) {
        Map<String, Stat> result = new HashMap<>();
        List<Object> fields = List.of("mean", "std", "count");
        List<String> missKeywords = new ArrayList<>();

        for (String keyword : keywords) {
            String redisKey = "keyword_stats:" + source + ":" + keyword;
            List<Object> cached = redisTemplate.opsForHash().multiGet(redisKey, fields);

            if (cached != null && cached.stream().noneMatch(Objects::isNull)) {
                result.put(keyword, new Stat(
                        Double.parseDouble(cached.get(0).toString()),
                        Double.parseDouble(cached.get(1).toString()),
                        Long.parseLong(cached.get(2).toString())
                ));
            } else {
                missKeywords.add(keyword);
            }
        }

        if (missKeywords.isEmpty()) return result;

        try {
            MgetResponse<Map> mgetResponse = esClient.mget(m -> m
                    .index("keyword-stats")
                    .ids(missKeywords), Map.class);

            for (MultiGetResponseItem<Map> doc : mgetResponse.docs()) {
                if (!doc.result().found() || doc.result().source() == null) continue;

                String keyword = doc.result().id();
                Map<String, Object> sourceMap = doc.result().source();

                double mean = ((Number) sourceMap.getOrDefault("mean", 0.0)).doubleValue();
                double std = ((Number) sourceMap.getOrDefault("std_dev", 1.0)).doubleValue();

                long count;
                if ("all".equals(source)) {
                    count = ((Number) sourceMap.getOrDefault("total_count", 1)).longValue();
                } else {
                    Map<String, Object> sources = (Map<String, Object>) sourceMap.get("sources");
                    count = (sources != null && sources.get(source) != null)
                            ? ((Number) sources.get(source)).longValue()
                            : 1L;
                }

                Stat stat = new Stat(mean, std, count);
                result.put(keyword, stat);

                String redisKey = "keyword_stats:" + source + ":" + keyword;
                redisTemplate.opsForHash().put(redisKey, "mean", String.valueOf(mean));
                redisTemplate.opsForHash().put(redisKey, "std", String.valueOf(std));
                redisTemplate.opsForHash().put(redisKey, "count", String.valueOf(count));
                redisTemplate.expire(redisKey, Duration.ofDays(2));
            }

        } catch (Exception e) {
            log.error("[ES][mget] Failed to fetch stats for source={}, keywords={}", source, missKeywords, e);
            for (String keyword : missKeywords) {
                result.put(keyword, new Stat(0.0, 1.0, 1L));
            }
        }

        return result;
    }

    private double calcTrendScore(int todayCount, int yesterdayCount, double mean, double std) {
        double growth = (double) (todayCount - yesterdayCount) / (ALPHA + todayCount);
        double standardized = (std == 0) ? 1.0 : (todayCount - mean) / std;
        return growth * standardized;
    }

    private record Stat(double mean, double std, long count) {}
}
