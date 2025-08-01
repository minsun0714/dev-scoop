package com.devscoop.api.crawler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class HackerNewsCrawler {

    private final ObjectMapper mapper;
    private final HttpClient client = HttpClient.newHttpClient();

    private static final String BASE_URL = "https://hacker-news.firebaseio.com/v0";
    private static final String USER_AGENT = "dev-scoop-crawler";

    public HackerNewsCrawler(ObjectMapper mapper) {
        this.mapper = mapper; // 스프링이 관리하는 ObjectMapper 주입
    }

    public JsonNode[] fetchTopStories(int count) {
        try {
            String idsJson = fetch(BASE_URL + "/topstories.json");
            JsonNode ids = mapper.readTree(idsJson);

            return IntStream.range(0, Math.min(count, ids.size()))
                    .mapToObj(i -> {
                        long id = ids.get(i).asLong();
                        HttpRequest req = HttpRequest.newBuilder()
                                .uri(URI.create(BASE_URL + "/item/" + id + ".json"))
                                .header("User-Agent", USER_AGENT)
                                .build();

                        return client.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                                .thenApply(HttpResponse::body)
                                .thenApply(body -> {
                                    try {
                                        return mapper.readTree(body);
                                    } catch (Exception e) {
                                        log.error("Error parsing story {}", id, e);
                                        return null;
                                    }
                                });
                    })
                    .toList() // CompletableFuture<JsonNode> 리스트
                    .stream()
                    .map(CompletableFuture::join)
                    .filter(Objects::nonNull)
                    .toArray(JsonNode[]::new);

        } catch (Exception e) {
            log.error("Failed to fetch HackerNews top stories", e);
            return new JsonNode[0];
        }
    }


    private String fetch(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", "dev-scoop-crawler")
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString()).body();
    }
}
