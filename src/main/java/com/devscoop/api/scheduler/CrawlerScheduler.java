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
import java.time.ZoneOffset;
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
        CompletableFuture<Void> devtoFuture = CompletableFuture.runAsync(this::crawlDevto, executor);
        CompletableFuture<Void> redditFuture = CompletableFuture.runAsync(this::crawlReddit, executor);

        CompletableFuture.allOf(hackerNewsFuture, devtoFuture, redditFuture).join();
    }

    private boolean isNewContent(String key, String value) {
        Long added = redisTemplate.opsForSet().add(key, value);
        if (added != null && added == 1) {
            redisTemplate.expire(key, Duration.ofDays(2));
            return true;
        }
        return false;
    }

    private void crawlHackerNews() {
        try {
            var stories = hackerNewsCrawler.fetchTopStories(10);
            for (RawPostDto story : stories) {
                String uniqueKey = story.url().isBlank() ? story.title() : story.url();
                if (!isNewContent("seen:hackernews", uniqueKey)) continue;

                RawPostDto dto = RawPostDto.builder()
                        .source(story.source())
                        .title(story.title())
                        .url(story.url())
                        .createdAt(story.createdAt())
                        .build();

                String json = mapper.writeValueAsString(dto);
                producer.send(TOPIC, "hackernews", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlHackerNews", e);
        }
    }

    private void crawlReddit() {
        try {
            var posts = redditCrawler.fetchTopPosts(10);
            for (RawPostDto post : posts) {
                String uniqueKey = post.url().isBlank() ? post.title() : post.url();
                if (!isNewContent("seen:reddit", uniqueKey)) continue;

                RawPostDto dto = RawPostDto.builder()
                        .source(post.source())
                        .title(post.title())
                        .url(post.url())
                        .createdAt(post.createdAt())
                        .build();

                String json = mapper.writeValueAsString(dto);
                producer.send(TOPIC, "reddit", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlReddit", e);
        }
    }

    private void crawlDevto() {
        try {
            var posts = devtoCrawler.fetchTopPosts(10);
            for (RawPostDto post : posts) {
                String uniqueKey = post.url().isBlank() ? post.title() : post.url();
                if (!isNewContent("seen:devto", uniqueKey)) continue;

                RawPostDto dto = RawPostDto.builder()
                        .source(post.source())
                        .title(post.title())
                        .url(post.url())
                        .createdAt(post.createdAt())
                        .build();

                String json = mapper.writeValueAsString(dto);
                producer.send(TOPIC, "devto", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlDevto", e);
        }
    }
}
