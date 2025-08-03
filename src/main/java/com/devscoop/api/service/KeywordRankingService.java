package com.devscoop.api.service;

import com.devscoop.api.dto.KeywordRankingDto;
import com.devscoop.api.entity.KeywordStat;
import com.devscoop.api.publisher.PreloadPublisher;
import com.devscoop.api.repository.KeywordStatRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class KeywordRankingService {

    private final static double ALPHA = 1.0;

    private final RedisTemplate<String, String> redisTemplate;
    private final KeywordStatRepository keywordStatRepository;
    private final PreloadPublisher preloadPublisher;

    public List<KeywordRankingDto> getKeywordRanking(String source, int limit){

        Map<String, Integer> todayCounts = fetchKeywordCounts(source, LocalDate.now(), limit);
        Map<String, Integer> yesterdayCounts = fetchKeywordCounts(source, LocalDate.now().minusDays(1), limit);

        return todayCounts.entrySet().stream()
                .map(entry -> {
                    String keyword = entry.getKey();
                    int todayCount = entry.getValue();
                    int yesterdayCount = yesterdayCounts.getOrDefault(keyword, 0);

                    KeywordStat keywordStat = getKeywordStats(source, keyword);

                    double score = calcTrendScore(todayCount, yesterdayCount, keywordStat.getMean(), keywordStat.getStd());

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

        // ZSET에서 상위 100개만 가져오기
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

    private KeywordStat getKeywordStats(String source, String keyword) {
        String key = "keyword_stats:" + source + ":" + keyword;
        List<Object> values = redisTemplate.opsForHash().multiGet(key, List.of("mean", "std"));

        // 1. Redis cache hit
        if (values.get(0) != null && values.get(1) != null) {
            return KeywordStat.of(
                    source,
                    keyword,
                    Double.parseDouble(values.get(0).toString()),
                    Double.parseDouble(values.get(1).toString())
            );
        }

        // 2. cache miss → DB fallback (동기)
        KeywordStat sourceStats = keywordStatRepository.findBySourceAndKeyword(source, keyword)
                .orElse(KeywordStat.of(source, keyword, 0.0, 1.0));

        // Redis에 source scope mean/std 비동기 큐 발행 전 데이터를 임시로 저장
        String sourceKey = "keyword_stats:" + source + ":" + keyword;
        redisTemplate.opsForHash().put(sourceKey, "mean", String.valueOf(sourceStats.getMean()));
        redisTemplate.opsForHash().put(sourceKey, "std", String.valueOf(sourceStats.getStd()));
        redisTemplate.expire(sourceKey, java.time.Duration.ofHours(24));

        // 3. 비동기 큐 발행
        preloadPublisher.publishPreloadRequest(source, keyword);
        if (!"all".equals(source)) preloadPublisher.publishPreloadRequest("all", keyword);

        return sourceStats;
    }

    private double calcTrendScore(int todayCount, int yesterdayCount, double mean, double std) {
        double growth = (double) (todayCount - yesterdayCount) / (ALPHA + todayCount);
        double standardized = (std == 0) ? 1.0 : (todayCount - mean) / std;
        return growth * standardized;
    }

}
