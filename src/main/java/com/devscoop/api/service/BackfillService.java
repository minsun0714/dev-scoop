package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.devscoop.api.crawler.DevtoCrawler;
import com.devscoop.api.crawler.HackerNewsCrawler;
import com.devscoop.api.crawler.RedditCrawler;
import com.devscoop.api.dto.RawPostDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class BackfillService {

    private final HackerNewsCrawler hackerNewsCrawler;
    private final RedditCrawler redditCrawler;
    private final DevtoCrawler devtoCrawler;
    private final ElasticsearchClient esClient;

    // 매일 오전 6시 3분 실행
    public void runBackfillJob() {
        LocalDateTime end = LocalDateTime.now();
        LocalDateTime start = end.minusMonths(6);

        log.info("Backfill job started: {} ~ {}", start, end);

        process("HackerNews", hackerNewsCrawler.fetchByDateRange(start, end)
                .stream()
                .filter(dto -> dto.createdAt() != null && !dto.createdAt().isBefore(start))
                .toList());

        process("Reddit", redditCrawler.fetchByDateRange(start, end));

        process("Dev.to", devtoCrawler.fetchByDateRange(start, end));

        log.info("Backfill job completed.");
    }

    private void process(String source, List<RawPostDto> posts) {
        log.info(">>> {} Step will process {} items", source, posts.size());
        for (RawPostDto dto : posts) {
            try {
                RawPostDto post = RawPostDto.builder()
                        .source(dto.source())
                        .title(dto.title())
                        .url(dto.url())
                        .createdAt(dto.createdAt())
                        .build();

                IndexRequest.Builder<RawPostDto> builder = new IndexRequest.Builder<RawPostDto>()
                        .index("raw-posts")
                        .document(post);

                if (post.url() != null && !post.url().isBlank()) {
                    builder.id(post.url());
                }

                esClient.index(builder.build());
            } catch (Exception e) {
                log.error("Failed to index post: url={} title={}", dto.url(), dto.title(), e);
            }
        }
    }
}
