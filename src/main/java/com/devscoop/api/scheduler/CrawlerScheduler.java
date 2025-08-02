package com.devscoop.api.scheduler;

import com.devscoop.api.crawler.DevtoCrawler;
import com.devscoop.api.crawler.HackerNewsCrawler;
import com.devscoop.api.crawler.RedditCrawler;
import com.devscoop.api.dto.RawPostDto;
import com.devscoop.api.producer.CrawledDataProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private final CrawledDataProducerService producer;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper mapper;

    private static final String TOPIC = "raw-posts";
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    @Scheduled(fixedRate = 300_000)
    public void crawlAll() {
        CompletableFuture<Void> hackerNewsFuture = CompletableFuture.runAsync(this::crawlHackerNews, executor);
        CompletableFuture<Void> githubFuture = CompletableFuture.runAsync(this::crawlDevto, executor);
        CompletableFuture<Void> redditFuture = CompletableFuture.runAsync(this::crawlReddit, executor);

        CompletableFuture.allOf(hackerNewsFuture, githubFuture, redditFuture).join();
    }

    /**
     * Redis에 key:value 저장 시도, 새로 추가되었으면 true 반환
     */
    private boolean isNewContent(String key, String value) {
        Long added = redisTemplate.opsForSet().add(key, value);
        if (added != null && added == 1) {
            redisTemplate.expire(key, Duration.ofDays(7));
            return true;
        }
        return false;
    }



    private void crawlHackerNews() {
        try {
            var stories = hackerNewsCrawler.fetchTopStories(10); // List<RawPostDto>
            for (RawPostDto story : stories) {
                // 중복 여부를 url 기준으로 체크
                String uniqueKey = story.getUrl().isBlank() ? story.getTitle() : story.getUrl();
                if (!isNewContent("seen:hackernews", uniqueKey)) continue;

                String json = mapper.createObjectNode()
                        .put("source", story.getSource())
                        .put("title", story.getTitle())
                        .put("url", story.getUrl())
                        .put("time", story.getPostedAt().toEpochSecond(java.time.ZoneOffset.UTC))
                        .toString();
                producer.send(TOPIC, "hackernews", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlHackerNews", e);
        }
    }

    private void crawlReddit() {
        try {
            var posts = redditCrawler.fetchTopPosts(10); // List<RawPostDto>
            for (RawPostDto post : posts) {
                String uniqueKey = post.getUrl().isBlank() ? post.getTitle() : post.getUrl();
                if (!isNewContent("seen:reddit", uniqueKey)) continue;

                String json = mapper.createObjectNode()
                        .put("source", post.getSource())
                        .put("title", post.getTitle())
                        .put("url", post.getUrl())
                        .put("time", post.getPostedAt().toEpochSecond(java.time.ZoneOffset.UTC))
                        .toString();
                producer.send(TOPIC, "reddit", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlReddit", e);
        }
    }

    private void crawlDevto() {
        try {
            var posts = devtoCrawler.fetchTopPosts(10); // List<RawPostDto>
            for (RawPostDto post : posts) {
                String uniqueKey = post.getUrl().isBlank() ? post.getTitle() : post.getUrl();
                if (!isNewContent("seen:devto", uniqueKey)) continue;

                String json = mapper.createObjectNode()
                        .put("source", post.getSource())
                        .put("title", post.getTitle())
                        .put("url", post.getUrl())
                        .put("time", post.getPostedAt().toEpochSecond(java.time.ZoneOffset.UTC))
                        .toString();
                producer.send(TOPIC, "devto", json);
            }
        } catch (Exception e) {
            log.error("Error during devto", e);
        }
    }
}
