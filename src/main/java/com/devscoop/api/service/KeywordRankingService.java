package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import com.devscoop.api.dto.KeywordRankingDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeywordRankingService {

    private static final double ALPHA = 1.0;
    private static final List<String> SOURCES = List.of("all", "github", "hackernews", "reddit");
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private final RedisTemplate<String, String> redisTemplate;
    private final ElasticsearchClient esClient;

    private List<Double> zscoreBatch(String key, List<String> members) {
        List<Object> raw = redisTemplate.executePipelined((RedisCallback<?>) conn -> {
            var s = conn.stringCommands(); // 필요없어도 호출해야 파이프라인 시작
            var z = conn.zSetCommands();
            for (String m : members) z.zScore(key.getBytes(StandardCharsets.UTF_8), m.getBytes(StandardCharsets.UTF_8));
            return null;
        });
        return raw.stream().map(o -> (Double) o).toList();
    }

    public List<KeywordRankingDto> getKeywordRanking(String source, int limit) {
        var todayCounts = fetchKeywordCounts(source, LocalDate.now(KST), limit); // 오늘은 top N만
        var todayKeywords = new ArrayList<>(todayCounts.keySet());

        String yKey = "keyword_count:" + source + ":" + LocalDate.now(KST).minusDays(1);
        var yScores = zscoreBatch(yKey, todayKeywords); // 어제는 오늘 키만 ZSCORE

        Map<String, Stat> statMap = fetchKeywordStatsBulk(source, new HashSet<>(todayKeywords));

        List<KeywordRankingDto> out = new ArrayList<>();
        for (int i = 0; i < todayKeywords.size(); i++) {
            String kw = todayKeywords.get(i);
            int today = todayCounts.get(kw);
            int yesterday = yScores.get(i) == null ? 0 : (int) Math.round(yScores.get(i));
            Stat st = statMap.getOrDefault(kw, new Stat(0.0, 1.0, 1L));
            double score = calcTrendScore(today, yesterday, st.mean, st.std);
            out.add(KeywordRankingDto.builder()
                    .keyword(kw).todayCount(today).yesterdayCount(yesterday).score(score).build());
        }

        return out.stream()
                .sorted(Comparator.comparingDouble(KeywordRankingDto::score).reversed())
                .limit(limit)
                .toList();
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
