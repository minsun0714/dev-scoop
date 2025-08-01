package com.devscoop.api.scheduler;

import com.devscoop.api.crawler.GitHubTrendingCrawler;
import com.devscoop.api.crawler.HackerNewsCrawler;
import com.devscoop.api.crawler.RedditCrawler;
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
    private final GitHubTrendingCrawler gitHubTrendingCrawler;
    private final RedditCrawler redditCrawler;
    private final CrawledDataProducerService producer;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper mapper;

    private static final String TOPIC = "raw-posts";
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    @Scheduled(fixedRate = 300_000)
    public void crawlAll() {
        CompletableFuture<Void> hackerNewsFuture = CompletableFuture.runAsync(this::crawlHackerNews, executor);
        CompletableFuture<Void> githubFuture = CompletableFuture.runAsync(this::crawlGitHubTrending, executor);
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
            var stories = hackerNewsCrawler.fetchTopStories(10);
            for (var story : stories) {
                String id = String.valueOf(story.path("id").asLong());
                if (!isNewContent("seen:hackernews", id)) continue;

                String json = mapper.createObjectNode()
                        .put("source", "hackernews")
                        .put("title", story.path("title").asText())
                        .put("url", story.path("url").asText(""))
                        .put("time", story.path("time").asLong())
                        .toString();
                producer.send(TOPIC, "hackernews", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlHackerNews", e);
        }
    }

    private void crawlGitHubTrending() {
        try {
            var repos = gitHubTrendingCrawler.fetchTrendingRepos();
            for (int i = 0; i < Math.min(5, repos.size()); i++) {
                var repo = repos.get(i);
                String url = "https://github.com" + repo.attr("href");
                if (!isNewContent("seen:github", url)) continue;

                String json = mapper.createObjectNode()
                        .put("source", "github")
                        .put("title", repo.text().trim())
                        .put("url", url)
                        .toString();
                producer.send(TOPIC, "github", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlGitHubTrending", e);
        }
    }

    private void crawlReddit() {
        try {
            var posts = redditCrawler.fetchTopPosts(5);
            for (var post : posts) {
                String postId = post.path("id").asText();
                if (!isNewContent("seen:reddit", postId)) continue;

                String json = mapper.createObjectNode()
                        .put("source", "reddit")
                        .put("title", post.path("title").asText())
                        .put("url", post.path("url").asText())
                        .toString();
                producer.send(TOPIC, "reddit", json);
            }
        } catch (Exception e) {
            log.error("Error during crawlReddit", e);
        }
    }
}
