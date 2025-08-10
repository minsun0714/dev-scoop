package com.devscoop.api.scheduler;

import com.devscoop.api.crawler.DevtoCrawler;
import com.devscoop.api.crawler.HackerNewsCrawler;
import com.devscoop.api.crawler.RedditCrawler;
import com.devscoop.api.dto.RawPostDto;
import com.devscoop.api.producer.CrawledDataProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Component
@RequiredArgsConstructor
public class CrawlerScheduler {

    private final HackerNewsCrawler hackerNewsCrawler;
    private final RedditCrawler redditCrawler;
    private final DevtoCrawler devtoCrawler;
    private final CrawledDataProducerService producer; // KafkaTemplate 래퍼(acks=all,idempotence=true 권장)
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper mapper;

    private static final String TOPIC = "raw-posts";
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    // 5분마다 / 앱 시작 30초 후 첫 실행
    @Scheduled(fixedRate = 300_000, initialDelay = 30_000)
    public void crawlAll() {
        CompletableFuture<Void> f1 = CompletableFuture.runAsync(() -> safeRun(this::crawlHackerNews, "hackernews"), executor);
        CompletableFuture<Void> f2 = CompletableFuture.runAsync(() -> safeRun(this::crawlDevto, "devto"), executor);
        CompletableFuture<Void> f3 = CompletableFuture.runAsync(() -> safeRun(this::crawlReddit, "reddit"), executor);
        CompletableFuture.allOf(f1, f2, f3).join();
    }

    private void safeRun(Runnable r, String name) {
        try { r.run(); }
        catch (Exception e) { log.error("crawl {} failed", name, e); }
    }

    private boolean isNewContent(String source, String urlOrTitle) {
        String normalized = normalize(urlOrTitle);
        String basis = source + "|" + normalized; // 소스까지 포함하고 싶으면 포함
        String key = "seen:url:" + sha256(basis);
        // NX + TTL (2일) — 같은 URL은 48시간 내 재통과 불가
        Boolean ok = redisTemplate.opsForValue().setIfAbsent(key, "1", java.time.Duration.ofDays(2));
        return Boolean.TRUE.equals(ok);
    }

    private String sha256(String s) {
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(h.length * 2);
            for (byte b : h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(s.hashCode());
        }
    }

    private String normalize(String s) {
        if (s == null) return "";
        String t = s.trim();
        int i = t.indexOf('?'); if (i > 0) t = t.substring(0, i); // 트래킹 파라미터 제거
        if (t.endsWith("/")) t = t.substring(0, t.length() - 1);
        return t;
    }

    private void crawlHackerNews() {
        var stories = retry(() -> hackerNewsCrawler.fetchTopStories(10), 3, 1000);
        for (RawPostDto story : stories) {
            String uniqueKey = (story.url() == null || story.url().isBlank()) ? story.title() : story.url();
            if (!isNewContent("hackernews", uniqueKey)) continue;
            publish("hackernews", story);
        }
    }

    private void crawlReddit() {
        var posts = retry(() -> redditCrawler.fetchTopPosts(10), 3, 1000);
        for (RawPostDto post : posts) {
            String uniqueKey = (post.url() == null || post.url().isBlank()) ? post.title() : post.url();
            if (!isNewContent("reddit", uniqueKey)) continue;
            publish("reddit", post);
        }
    }

    private void crawlDevto() {
        var posts = retry(() -> devtoCrawler.fetchTopPosts(10), 3, 1000);
        for (RawPostDto post : posts) {
            String uniqueKey = (post.url() == null || post.url().isBlank()) ? post.title() : post.url();
            if (!isNewContent("devto", uniqueKey)) continue;
            publish("devto", post);
        }
    }

    private void publish(String source, RawPostDto dto) {
        try {
            // date_kst 수집 시점 기준으로 세팅
            String dateKst = java.time.LocalDate.now(java.time.ZoneId.of("Asia/Seoul")).toString();

            RawPostDto enriched = RawPostDto.builder()
                    .source(dto.source())
                    .title(dto.title())
                    .url(dto.url())
                    .createdAt(dto.createdAt())
                    .dateKst(dateKst)
                    .keywords(dto.keywords())
                    .build();

            var json = mapper.writeValueAsString(enriched);

            // key=URL(멱등/파티셔닝), 없으면 제목 fallback
            String key = (dto.url() == null || dto.url().isBlank())
                    ? dto.title()
                    : normalize(dto.url());

            producer.send(TOPIC, key, json);
        } catch (Exception e) {
            log.error("produce failed: source={} url={} title={}",
                    source, dto.url(), dto.title(), e);
        }
    }


    private <T> T retry(java.util.concurrent.Callable<T> call, int times, long backoffMs) {
        int n = 0; Throwable last = null;
        while (n++ < times) {
            try { return call.call(); }
            catch (Throwable t) { last = t; sleep(backoffMs * n); }
        }
        throw new RuntimeException("retry failed", last);
    }

    private void sleep(long ms) { try { Thread.sleep(ms); } catch (InterruptedException ignored) {} }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }
}
