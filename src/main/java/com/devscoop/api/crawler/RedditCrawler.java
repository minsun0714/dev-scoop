package com.devscoop.api.crawler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

@Slf4j
@Component
@RequiredArgsConstructor
public class RedditCrawler {

    private static final String BASE_URL = "https://www.reddit.com/r/programming/top.json";
    private static final String POST_DETAIL_URL = "https://www.reddit.com/by_id/"; // 상세 조회용 예시 URL
    private static final String USER_AGENT = "dev-scoop-crawler";

    private final ObjectMapper mapper;
    private final HttpClient client = HttpClient.newHttpClient();

    public JsonNode[] fetchTopPosts(int count) {
        try {
            // 1. Top posts 리스트 가져오기
            String url = BASE_URL + "?limit=" + count + "&t=day";
            String json = fetch(url);
            JsonNode posts = mapper.readTree(json).path("data").path("children");
            int limit = Math.min(count, posts.size());

            // 2. 각 post를 병렬로 상세 데이터 요청 (sendAsync)
            return IntStream.range(0, limit)
                    .mapToObj(i -> posts.get(i).get("data"))
                    .map(post -> {
                        String postId = post.path("name").asText(); // e.g., t3_xxxxxx
                        String detailUrl = POST_DETAIL_URL + postId + ".json";

                        HttpRequest req = HttpRequest.newBuilder()
                                .uri(URI.create(detailUrl))
                                .header("User-Agent", USER_AGENT)
                                .build();

                        // 비동기 요청
                        return client.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                                .thenApply(HttpResponse::body)
                                .thenApply(body -> {
                                    try {
                                        // Reddit 상세 JSON 파싱
                                        return mapper.readTree(body);
                                    } catch (Exception e) {
                                        log.error("Failed to parse Reddit post {}", postId, e);
                                        return null;
                                    }
                                });
                    })
                    .map(CompletableFuture::join) // 모든 비동기 요청 완료 대기
                    .filter(Objects::nonNull)
                    .toArray(JsonNode[]::new);

        } catch (Exception e) {
            log.error("Failed to fetch Reddit top posts", e);
            return new JsonNode[0];
        }
    }

    private String fetch(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", USER_AGENT)
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }
}
