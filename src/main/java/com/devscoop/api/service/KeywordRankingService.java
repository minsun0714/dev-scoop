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

    private static final double ALPHA = 1.0;                // today 분모 안정화
    private static final double EPS = 1e-6;                 // 0-division 가드
    private static final double MAX_Z = 5.0;                // z-score 상한
    private static final double SPIKE_SCORE  = 3.0;
    private static final double RISING_SCORE = 1.5;
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private final RedisTemplate<String, String> redisTemplate;
    private final ElasticsearchClient esClient;

    /**
     * Redis ZSCORE 파이프라인: members 순서 그대로 Double(or null) 리스트 반환
     */
    private List<Double> zscoreBatch(String key, List<String> members) {
        if (members.isEmpty()) return Collections.emptyList();

        List<Object> raw = redisTemplate.executePipelined((RedisCallback<?>) conn -> {
            var z = conn.zSetCommands();
            byte[] k = key.getBytes(StandardCharsets.UTF_8);
            for (String m : members) {
                if (m == null) continue;
                z.zScore(k, m.getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });

        List<Double> out = new ArrayList<>(raw.size());
        for (Object o : raw) out.add(o == null ? null : (Double) o);
        return out;
    }

    public List<KeywordRankingDto> getKeywordRanking(String source, int limit) {
        LocalDate today = LocalDate.now(KST);
        LocalDate yesterday = today.minusDays(1);

        // 오늘 top N (조금 넉넉히 가져와서 이후 정렬 후 limit)
        var todayCounts = fetchKeywordCounts(source, today, Math.max(limit, 20));
        var todayKeywords = new ArrayList<>(todayCounts.keySet()); // LinkedHashMap → 순서 보존

        // 어제 점수(오늘 키만 조회)
        String yKey = "keyword_count:" + source + ":" + yesterday;
        var yScores = zscoreBatch(yKey, todayKeywords);

        // 통계(mean/std) 벌크 조회 (캐시→ES mget 순)
        Map<String, Stat> statMap = fetchKeywordStatsBulk(source, new HashSet<>(todayKeywords));

        List<KeywordRankingDto> out = new ArrayList<>();
        for (int i = 0; i < todayKeywords.size(); i++) {
            String kw = todayKeywords.get(i);
            int todayCnt = todayCounts.getOrDefault(kw, 0);
            int yCnt = (i < yScores.size() && yScores.get(i) != null)
                    ? (int) Math.round(yScores.get(i)) : 0;

            Stat st = statMap.getOrDefault(kw, new Stat(0.0, 1.0, 1L));
            double score = calcTrendScore(todayCnt, yCnt, st.mean, st.std);
            String badge = resolveBadge(yCnt, score);

            out.add(KeywordRankingDto.builder()
                    .keyword(kw)
                    .todayCount(todayCnt)
                    .yesterdayCount(yCnt)
                    .score(score)
                    .badge(badge)
                    .build());
        }

        return out.stream()
                .sorted(Comparator.comparingDouble(KeywordRankingDto::score).reversed())
                .limit(limit)
                .toList();
    }

    /**
     * 오늘/특정 날짜의 상위 키워드 카운트 조회 (순서 보존)
     */
    private Map<String, Integer> fetchKeywordCounts(String source, LocalDate date, int limit) {
        String key = "keyword_count:" + source + ":" + date;

        Set<ZSetOperations.TypedTuple<String>> tuples =
                redisTemplate.opsForZSet().reverseRangeWithScores(key, 0, limit * 2L);

        Map<String, Integer> result = new LinkedHashMap<>(); // ★ 순서 유지
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

    /**
     * mean/std 통계 벌크 조회: Redis 캐시 → ES mget
     * ES는 mean_7d/std_dev_7d 또는 mean/std_dev 를 모두 지원
     */
    private Map<String, Stat> fetchKeywordStatsBulk(String source, Set<String> keywords) {
        Map<String, Stat> result = new HashMap<>();
        List<Object> fields = List.of("mean", "std", "count");
        List<String> misses = new ArrayList<>();

        // 1) Redis 캐시
        for (String keyword : keywords) {
            String rKey = "keyword_stats:" + source + ":" + keyword;
            List<Object> cached = redisTemplate.opsForHash().multiGet(rKey, fields);
            if (cached != null && cached.stream().noneMatch(Objects::isNull)) {
                try {
                    result.put(keyword, new Stat(
                            Double.parseDouble(cached.get(0).toString()),
                            Double.parseDouble(cached.get(1).toString()),
                            Long.parseLong(cached.get(2).toString())
                    ));
                } catch (Exception ignore) {
                    misses.add(keyword);
                }
            } else {
                misses.add(keyword);
            }
        }
        if (misses.isEmpty()) return result;

        // 2) ES mget
        try {
            MgetResponse<Map> mget = esClient.mget(m -> m.index("keyword-stats").ids(misses), Map.class);
            for (MultiGetResponseItem<Map> item : mget.docs()) {
                if (!item.result().found() || item.result().source() == null) continue;

                String kw = item.result().id();
                Map<String, Object> src = item.result().source();

                // mean/std 필드 양쪽 호환
                double mean = asDouble(src, "mean_7d",
                        asDouble(src, "mean", 0.0));
                double std  = asDouble(src, "std_dev_7d",
                        asDouble(src, "std_dev", 1.0));

                long count;
                if ("all".equals(source)) {
                    count = ((Number) src.getOrDefault("total_count", 1)).longValue();
                } else {
                    Map<String, Object> sources = (Map<String, Object>) src.get("sources");
                    count = (sources != null && sources.get(source) != null)
                            ? ((Number) sources.get(source)).longValue()
                            : 1L;
                }

                Stat st = new Stat(mean, std, count);
                result.put(kw, st);

                // 캐시 저장
                String rKey = "keyword_stats:" + source + ":" + kw;
                redisTemplate.opsForHash().put(rKey, "mean", String.valueOf(mean));
                redisTemplate.opsForHash().put(rKey, "std", String.valueOf(std));
                redisTemplate.opsForHash().put(rKey, "count", String.valueOf(count));
                redisTemplate.expire(rKey, Duration.ofDays(2));
            }
        } catch (Exception e) {
            log.error("[ES][mget] stats fetch failed for source={}, keywords={}", source, misses, e);
            for (String kw : misses) result.putIfAbsent(kw, new Stat(0.0, 1.0, 1L));
        }

        return result;
    }

    private double asDouble(Map<String, Object> src, String key, double def) {
        Object v = src.get(key);
        return (v instanceof Number n) ? n.doubleValue() : def;
    }

    /**
     * score = growth * z * log1p(today)
     *  - growth = (today - yesterday)/(today + ALPHA), [0,1]로 클램프
     *  - z = (today - mean)/sigma, sigma=std or sqrt(mean) 포아송 근사, [0, MAX_Z]로 클램프
     *  - log1p(today)로 절대 규모 가중
     */
    private double calcTrendScore(int today, int yesterday, double mean, double std) {
        if (today <= 0) return 0.0;

        double growth = (today - (double) yesterday) / (today + ALPHA);
        growth = Math.max(0.0, Math.min(1.0, growth));

        double sigma = (std > EPS) ? std : Math.sqrt(Math.max(mean, 1.0));
        double z = (today - mean) / Math.max(sigma, 1.0);
        z = Math.max(0.0, Math.min(MAX_Z, z));

        double w = Math.log1p(today); // 작은 값 튐 완화

        return growth * z * w;
    }

    private String resolveBadge(int yesterdayCount, double score) {
        // 어제 0이면 퍼센트 왜곡 대신 New로 명확히 표시
        if (yesterdayCount == 0 && score > EPS) return "New";

        // 급등: 점수 높을 때
        if (score >= SPIKE_SCORE) return "Spike";

        // 뚜렷한 상승
        if (score >= RISING_SCORE) return "Rising";

        // 배지 없음
        return null;
    }

    private record Stat(double mean, double std, long count) {}
}
