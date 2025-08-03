package com.devscoop.api.consumer;

import com.devscoop.api.entity.KeywordStat;
import com.devscoop.api.repository.KeywordStatRepository;
import com.devscoop.api.repository.RawPostRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class PreloadConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private final KeywordStatRepository keywordStatRepository;
    private final RawPostRepository rawPostRepository;
    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String source = message.getValue().get("source");
        String keyword = message.getValue().get("keyword");

        log.info("PreloadConsumer: preload request received - source={}, keyword={}", source, keyword);

        // 1. mean / std 계산
        double[] stats = calculateStats(source, keyword);
        double mean = stats[0];
        double std = stats[1];

        // 2. DB 업데이트
        KeywordStat stat = keywordStatRepository
                .findBySourceAndKeyword(source, keyword)
                .orElse(KeywordStat.of(source, keyword, mean, std));
        stat.updateStats(mean, std);
        keywordStatRepository.save(stat);

        // 3. Redis 캐시 갱신
        String redisKey = "keyword_stats:" + source + ":" + keyword;
        redisTemplate.opsForHash().put(redisKey, "mean", String.valueOf(mean));
        redisTemplate.opsForHash().put(redisKey, "std", String.valueOf(std));
        redisTemplate.expire(redisKey, Duration.ofHours(24));

        log.info("PreloadConsumer: updated DB & Redis with mean={}, std={} for {}:{}", mean, std, source, keyword);
    }

    private double[] calculateStats(String source, String keyword) {
        // DB에서 최근 30일간 일별 count를 기반으로 mean/std 계산
        Double[] result = rawPostRepository.calcMeanAndStd(source, keyword);

        if (result == null || result.length < 2 || result[0] == null || result[1] == null) {
            return new double[]{0.0, 1.0}; // std=1.0으로 분모 0 방지
        }

        return new double[]{result[0], result[1]};
    }

}
