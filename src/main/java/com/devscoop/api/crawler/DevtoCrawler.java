package com.devscoop.api.crawler;

import com.devscoop.api.dto.RawPostDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class DevtoCrawler {

    private static final String API_URL = "https://dev.to/api/articles";
    private static final String USER_AGENT = "Mozilla/5.0 dev-scoop-crawler";

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient client = HttpClient.newHttpClient();

    /**
     * 최근 하루 인기글 (Top posts)
     */
    public List<RawPostDto> fetchTopPosts(int count) {
        List<RawPostDto> results = new ArrayList<>();

        int page = 1;
        while (results.size() < count) {
            try {
                String url = String.format("%s?page=%d&per_page=100", API_URL, page);
                String body = fetch(url);

                JsonNode root = mapper.readTree(body);
                if (!root.isArray() || root.size() == 0) break;

                for (JsonNode item : root) {
                    if (results.size() >= count) break;
                    String title = item.path("title").asText();
                    String urlLink = item.path("url").asText();
                    String publishedAt = item.path("published_at").asText();
                    LocalDateTime postedAt = LocalDateTime.ofInstant(
                            Instant.parse(publishedAt), ZoneOffset.UTC
                    );

                    results.add(RawPostDto.builder()
                            .source("devto")
                            .title(title)
                            .url(urlLink)
                            .postedAt(postedAt)
                            .build());
                }
                page++;
            } catch (Exception e) {
                log.error("Failed to fetch Dev.to top posts", e);
                break;
            }
        }

        log.info("Fetched {} Dev.to trending posts (24h)", results.size());
        return results;
    }

    /**
     * 6개월 데이터: 날짜 필터링 (published_at)
     */
    public List<RawPostDto> fetchByDateRange(LocalDateTime start, LocalDateTime end) {
        List<RawPostDto> results = new ArrayList<>();

        int page = 1;
        while (true) {
            try {
                String url = String.format("%s?page=%d&per_page=100", API_URL, page);
                String body = fetch(url);

                JsonNode root = mapper.readTree(body);
                if (!root.isArray() || root.size() == 0) break;

                boolean reachedOldData = false;
                for (JsonNode item : root) {
                    String publishedAt = item.path("published_at").asText();
                    LocalDateTime postedAt = LocalDateTime.ofInstant(
                            Instant.parse(publishedAt), ZoneOffset.UTC
                    );

                    if (postedAt.isBefore(start)) {
                        reachedOldData = true;
                        break;
                    }

                    if (!postedAt.isAfter(end)) {
                        results.add(RawPostDto.builder()
                                .source("devto")
                                .title(item.path("title").asText())
                                .url(item.path("url").asText())
                                .postedAt(postedAt)
                                .build());
                    }
                }

                if (reachedOldData) break;
                page++;
            } catch (Exception e) {
                log.error("Failed to fetch Dev.to data", e);
                break;
            }
        }

        log.info("Fetched {} Dev.to posts between {} and {}", results.size(), start, end);
        return results;
    }

    private String fetch(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", USER_AGENT)
                .GET()
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }
}
