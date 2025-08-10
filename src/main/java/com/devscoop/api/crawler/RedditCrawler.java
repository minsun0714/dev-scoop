package com.devscoop.api.crawler;

import com.devscoop.api.dto.RawPostDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
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

    // OAuth 도메인 사용
    private static final String BASE_URL = "https://oauth.reddit.com/r/programming/top?raw_json=1";
    private static final String USER_AGENT_FMT = "DevScoopOAuthClient/1.0 by u/%s https://dev-scoop.click";

    private final ObjectMapper mapper;
    private final HttpClient client = HttpClient.newHttpClient();
    private final StringRedisTemplate redis; // Redis에서 refresh_token 읽기용

    @Value("${reddit.client-id}") private String CLIENT_ID;
    @Value("${reddit.secret}")    private String CLIENT_SECRET;
    @Value("${reddit.username}")  private String redditUsername;

    /** Reddit Top Posts (최근 하루) */
    public List<RawPostDto> fetchTopPosts(int count) {
        try {
            String url = BASE_URL + "&limit=" + count + "&t=day";
            String accessToken = getAccessTokenViaRefresh(); // 항상 토큰 붙여 호출

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Authorization", "Bearer " + accessToken)
                    .header("User-Agent", String.format(USER_AGENT_FMT, redditUsername))
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());

            // HTML 차단 페이지 방지용 가드
            String ctype = res.headers().firstValue("content-type").orElse("");
            String body  = res.body();
            if (!ctype.contains("application/json") || (body != null && !body.isEmpty() && body.charAt(0) == '<')) {
                log.error("[Reddit] Non-JSON response. status={}, content-type={}, head={}",
                        res.statusCode(), ctype, body.substring(0, Math.min(200, body.length())));
                return List.of();
            }

            JsonNode posts = mapper.readTree(body).path("data").path("children");
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

    /** 날짜 범위 수집도 동일하게 oauth 도메인 + 토큰 방식 유지 */
    public List<RawPostDto> fetchByDateRange(LocalDateTime start, LocalDateTime end) {
        List<RawPostDto> results = new ArrayList<>();
        try {
            String token = getAccessTokenViaRefresh();
            String after = null;
            long fromEpoch = start.toEpochSecond(ZoneOffset.UTC);

            while (true) {
                String url = "https://oauth.reddit.com/r/programming/new?limit=100" +
                        (after != null ? "&after=" + URLEncoder.encode(after, StandardCharsets.UTF_8) : "");

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Authorization", "Bearer " + token)
                        .header("User-Agent", String.format(USER_AGENT_FMT, redditUsername))
                        .header("Accept", "application/json")
                        .GET()
                        .build();

                HttpResponse<String> res = client.send(request, HttpResponse.BodyHandlers.ofString());
                String ctype = res.headers().firstValue("content-type").orElse("");
                String body  = res.body();
                if (!ctype.contains("application/json") || (body != null && !body.isEmpty() && body.charAt(0) == '<')) {
                    log.warn("[Reddit] Non-JSON while paging. status={}, ctype={}, head={}",
                            res.statusCode(), ctype, body.substring(0, Math.min(200, body.length())));
                    break;
                }

                JsonNode root = mapper.readTree(body);
                JsonNode children = root.path("data").path("children");
                if (!children.isArray() || children.size() == 0) break;

                boolean reachedOld = false;
                for (JsonNode child : children) {
                    JsonNode data = child.path("data");
                    long createdUtc = data.path("created_utc").asLong();
                    if (createdUtc < fromEpoch) { reachedOld = true; break; }

                    results.add(RawPostDto.builder()
                            .source("reddit")
                            .title(data.path("title").asText())
                            .url("https://reddit.com" + data.path("permalink").asText())
                            .createdAt(toLocalDateTime(createdUtc))
                            .build());
                }

                after = root.path("data").path("after").asText(null);
                if (after == null || reachedOld) break;

                Thread.sleep(1000); // rate limit 보호
            }

        } catch (Exception e) {
            log.error("Failed to fetch Reddit posts", e);
        }
        log.info("Fetched total {} Reddit posts between {} and {}", results.size(), start, end);
        return results;
    }

    /** Redis에 저장된 refresh_token으로 access_token 재발급 */
    private String getAccessTokenViaRefresh() throws Exception {
        String refreshToken = redis.opsForValue().get("reddit_refresh_token");
        if (refreshToken == null || refreshToken.isBlank()) {
            throw new IllegalStateException("No reddit_refresh_token in Redis. Finish OAuth first.");
        }

        String basicAuth = Base64.getEncoder()
                .encodeToString((CLIENT_ID + ":" + CLIENT_SECRET).getBytes(StandardCharsets.UTF_8));

        String body = "grant_type=refresh_token&refresh_token=" +
                URLEncoder.encode(refreshToken, StandardCharsets.UTF_8);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("https://www.reddit.com/api/v1/access_token"))
                .header("Authorization", "Basic " + basicAuth)
                .header("User-Agent", String.format(USER_AGENT_FMT, redditUsername))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() / 100 != 2) {
            log.error("[Reddit] refresh_token exchange failed: status={}, body={}", res.statusCode(), res.body());
            throw new RuntimeException("Reddit token refresh failed");
        }

        JsonNode root = mapper.readTree(res.body());
        JsonNode tokenNode = root.get("access_token");
        if (tokenNode == null) {
            log.error("[Reddit] No access_token in response: {}", res.body());
            throw new RuntimeException("No access_token");
        }
        return tokenNode.asText();
    }

    private LocalDateTime toLocalDateTime(long epochSeconds) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneOffset.UTC);
    }
}
