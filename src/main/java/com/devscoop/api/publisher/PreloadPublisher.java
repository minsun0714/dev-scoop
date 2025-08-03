package com.devscoop.api.publisher;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class PreloadPublisher {

    private final StringRedisTemplate redisTemplate;

    private static final String STREAM_KEY = "stream:keyword-preload";

    public void publishPreloadRequest(String source, String keyword) {
        Map<String, String> fields = Map.of(
                "source", source,
                "keyword", keyword
        );

        redisTemplate.opsForStream().add(STREAM_KEY, fields);
    }
}
