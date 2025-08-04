package com.devscoop.api.listener;

import com.devscoop.api.service.KeywordStatUpdateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RedisKeyExpirationListener implements MessageListener {

    private final KeywordStatUpdateService statUpdateService;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String expiredKey = message.toString();
        log.info("[Redis] TTL expired key: {}", expiredKey);

        if (expiredKey.startsWith("keyword_stats:")) {
            String keyword = expiredKey.substring("keyword_stats:".length());
            statUpdateService.updateMeanAndStd(keyword);
        }
    }
}
