package com.devscoop.api.crawler;

import com.devscoop.api.dto.RawPostDto;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
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
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

@Slf4j
@Component
public class HackerNewsCrawler {

    private final ObjectMapper mapper;
    private final HttpClient client = HttpClient.newHttpClient();

    private static final String BASE_URL = "https://hacker-news.firebaseio.com/v0";
    private static final String USER_AGENT = "dev-scoop-crawler-hackernews";
    private static final String ALGOLIA_API = "https://hn.algolia.com/api/v1/search_by_date";

    public HackerNewsCrawler(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * 최신 topstories 기반으로 상위 count개만 가져옴
     */
    public List<RawPostDto> fetchTopStories(int count) {
        try {
            var ids = getTopStoryIds();
            return ids.stream()
                    .limit(count)
                    .map(this::fetchItem)
                    .filter(Objects::nonNull)
                    .map(this::toDto)
                    .toList();
        } catch (Exception e) {
            log.error("Failed to fetch HackerNews top stories", e);
            return List.of();
        }
    }
    public List<RawPostDto> fetchByDateRange(LocalDateTime start, LocalDateTime end) {
        List<RawPostDto> results = new ArrayList<>();

        // 1개월 단위로 나누어 호출 (6개월이면 6번)
        LocalDateTime cursor = start;
        while (cursor.isBefore(end)) {
            LocalDateTime next = cursor.plusMonths(1);
            fetchFromAlgolia(cursor, next, results);
            cursor = next;
        }

        log.info("Fetched {} HackerNews posts between {} and {}",
                results.size(), start, end);

        return results;
    }

    /**
     * Algolia API를 사용하여 start~end 범위 데이터를 모두 페이지네이션으로 가져온다.
     */
    private void fetchFromAlgolia(LocalDateTime start, LocalDateTime end, List<RawPostDto> collector) {
        long startEpoch = start.toEpochSecond(ZoneOffset.UTC);
        long endEpoch = end.toEpochSecond(ZoneOffset.UTC);

        int page = 0;
        while (true) {
            try {
                // 쿼리 파라미터 구성
                String numericFilters = URLEncoder.encode(
                        "created_at_i>=" + startEpoch + ",created_at_i<=" + endEpoch,
                        StandardCharsets.UTF_8
                );

                String url = String.format(
                        "%s?tags=story&numericFilters=%s&hitsPerPage=1000&page=%d",
                        ALGOLIA_API, numericFilters, page
                );

                String json = fetch(url);
                JsonNode root = mapper.readTree(json);
                JsonNode hits = root.path("hits");

                if (hits.isEmpty()) {
                    break;
                }

                // 데이터 수집
                for (JsonNode hit : hits) {
                    collector.add(toDtoFromAlgolia(hit));
                }

                int currentPage = root.path("page").asInt();
                int totalPages = root.path("nbPages").asInt();

                log.info("[{} ~ {}] page {}/{} fetched {} items",
                        start, end, currentPage, totalPages, hits.size());

                // 마지막 페이지까지 다 읽었으면 중단
                if (currentPage >= totalPages - 1) {
                    break;
                }

                page++;
            } catch (Exception e) {
                log.error("Failed to fetch HackerNews data", e);
                break;
            }
        }
    }
    private RawPostDto toDtoFromAlgolia(JsonNode node) {
        String createdAt = node.path("created_at").asText();
        // "2025-02-01T12:34:56.000Z" -> LocalDateTime
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.parse(createdAt),
                ZoneOffset.UTC
        );

        return RawPostDto.builder()
                .source("hackernews")
                .title(node.path("title").asText())
                .url(node.path("url").asText(""))
                .postedAt(dateTime)
                .build();
    }

    private JsonNode fetchItem(long id) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(BASE_URL + "/item/" + id + ".json"))
                    .header("User-Agent", USER_AGENT)
                    .build();
            String body = client.send(req, HttpResponse.BodyHandlers.ofString()).body();
            return mapper.readTree(body);
        } catch (Exception e) {
            log.warn("Failed to fetch story {}", id, e);
            return null;
        }
    }

    private List<Long> getTopStoryIds() throws Exception {
        String idsJson = fetch(BASE_URL + "/topstories.json");
        JsonNode ids = mapper.readTree(idsJson);
        return IntStream.range(0, ids.size())
                .mapToObj(i -> ids.get(i).asLong())
                .toList();
    }

    private RawPostDto toDto(JsonNode node) {
        return RawPostDto.builder()
                .source("hackernews")
                .title(node.path("title").asText())
                .url(node.path("url").asText(""))
                .postedAt(toLocalDateTime(node.path("time").asLong()))
                .build();
    }

    private String fetch(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("User-Agent", USER_AGENT)
                .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString()).body();
    }

    private LocalDateTime toLocalDateTime(long epochSeconds) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneOffset.UTC);
    }
}
