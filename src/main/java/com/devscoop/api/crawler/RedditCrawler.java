package com.devscoop.api.crawler;

import com.devscoop.api.dto.RawPostDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedditCrawler {

    private static final String BASE_URL = "https://www.reddit.com/r/programming/top.json";
    private static final String USER_AGENT = "dev-scoop-crawler";

    private final ObjectMapper mapper;
    private final HttpClient client = HttpClient.newHttpClient();

    @Value("${REDDIT_CLIENT_ID}")
    private String CLIENT_ID;

    @Value("${REDDIT_SECRET}")
    private String CLIENT_SECRET;

    @Value("${REDDIT_USERNAME}")
    private String redditUsername;

    @Value("${REDDIT_PASSWORD}")
    private String redditPassword;

    /**
     * Reddit Top Posts (최근 하루)
     */
    public List<RawPostDto> fetchTopPosts(int count) {
        try {
            String url = BASE_URL + "?limit=" + count + "&t=day";
            String json = fetch(url);
            JsonNode posts = mapper.readTree(json).path("data").path("children");
            int limit = Math.min(count, posts.size());

            return IntStream.range(0, limit)
                    .mapToObj(i -> posts.get(i).get("data"))
                    .filter(Objects::nonNull)
                    .map(post -> RawPostDto.builder()
                            .source("reddit")
                            .title(post.path("title").asText())
                            .url("https://reddit.com" + post.path("permalink").asText())
                            .createdAt(toLocalDateTime(post.path("created_utc").asLong()))
                            .build())
                    .toList();

        } catch (Exception e) {
            log.error("Failed to fetch Reddit top posts", e);
            return List.of();
        }
    }

    private String fetch(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", USER_AGENT)
                .GET()
                .build();

        return client.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }


    public List<RawPostDto> fetchByDateRange(LocalDateTime start, LocalDateTime end) {
        List<RawPostDto> results = new ArrayList<>();

        try {
            String token = getAccessToken();
            String after = null;

            long sixMonthsAgoEpoch = start.toEpochSecond(ZoneOffset.UTC);

            while (true) {
                String url = "https://oauth.reddit.com/r/programming/new?limit=100";
                if (after != null) {
                    url += "&after=" + URLEncoder.encode(after, java.nio.charset.StandardCharsets.UTF_8);
                }

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Authorization", "bearer " + token)
                        .header("User-Agent", USER_AGENT)
                        .GET()
                        .build();

                String body = client.send(request, HttpResponse.BodyHandlers.ofString()).body();
                JsonNode root = mapper.readTree(body);
                JsonNode children = root.path("data").path("children");

                if (!children.isArray() || children.size() == 0) {
                    break; // 더 이상 데이터 없음
                }

                boolean reachedOldData = false;

                for (JsonNode child : children) {
                    JsonNode data = child.path("data");
                    long createdUtc = data.path("created_utc").asLong();
                    if (createdUtc < sixMonthsAgoEpoch) {
                        reachedOldData = true;
                        break;
                    }

                    results.add(
                            RawPostDto.builder()
                                    .source("reddit")
                                    .title(data.path("title").asText())
                                    .url("https://reddit.com" + data.path("permalink").asText())
                                    .createdAt(toLocalDateTime(createdUtc))
                                    .build()
                    );
                }

                after = root.path("data").path("after").asText(null);
                if (after == null || reachedOldData) {
                    break;
                }

                log.info("Fetched {} Reddit posts so far. Continuing...", results.size());
                Thread.sleep(1000); // API rate limit 대비
            }

        } catch (Exception e) {
            log.error("Failed to fetch Reddit posts", e);
        }

        log.info("Fetched total {} Reddit posts between {} and {}", results.size(), start, end);
        return results;
    }

    private String getAccessToken() throws Exception {
        String auth = CLIENT_ID + ":" + CLIENT_SECRET;
        String basicAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://www.reddit.com/api/v1/access_token"))
                .header("Authorization", "Basic " + basicAuth)
                .header("User-Agent", USER_AGENT)
                .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials"))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        log.info("Reddit token response: {}", response.body()); // 디버깅용 로그

        JsonNode root = mapper.readTree(response.body());
        JsonNode tokenNode = root.get("access_token");
        if (tokenNode == null) {
            throw new RuntimeException("Failed to get access token: " + response.body());
        }
        return tokenNode.asText();
    }


    private LocalDateTime toLocalDateTime(long epochSeconds) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneOffset.UTC);
    }
}
