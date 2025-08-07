package com.devscoop.api.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import com.devscoop.api.crawler.*;
import com.devscoop.api.dto.RawPostDto;
import com.devscoop.api.reader.IteratorItemReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BackfillJobConfig {

    private final HackerNewsCrawler hackerNewsCrawler;
    private final RedditCrawler redditCrawler;
    private final DevtoCrawler devtoCrawler;
    private final ElasticsearchClient esClient;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job backfillJob() {
        return new JobBuilder("backfillJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(hackerNewsStep())
                .next(redditStep())
                .next(devtoStep())
                .build();
    }

    @Bean
    public Step hackerNewsStep() {
        LocalDateTime end = LocalDateTime.now();
        LocalDateTime start = end.minusMonths(6);

        List<RawPostDto> dtos = hackerNewsCrawler.fetchByDateRange(start, end)
                .stream()
                .filter(dto -> dto.createdAt() != null && !dto.createdAt().isBefore(start))
                .toList();

        log.info(">>> HackerNews Step will process {} items", dtos.size());

        return new StepBuilder("hackerNewsStep", jobRepository)
                .<RawPostDto, RawPostDto>chunk(1000, transactionManager)
                .reader(new IteratorItemReader<>(dtos.iterator()))
                .processor(dto -> RawPostDto.builder()
                        .source(dto.source())
                        .title(dto.title())
                        .url(dto.url())
                        .createdAt(dto.createdAt())
                        .build())
                .writer(items -> {
                    log.info(">>> Writing {} items to Elasticsearch", items.size());
                    for (RawPostDto post : items) {
                        indexDocument(post);
                    }
                })
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Step redditStep() {
        return new StepBuilder("redditStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    LocalDateTime end = LocalDateTime.now();
                    LocalDateTime start = end.minusMonths(6);

                    List<RawPostDto> dtos = redditCrawler.fetchByDateRange(start, end);

                    for (RawPostDto dto : dtos) {
                        RawPostDto post = RawPostDto.builder()
                                .source(dto.source())
                                .title(dto.title())
                                .url(dto.url())
                                .createdAt(dto.createdAt())
                                .build();
                        indexDocument(post);
                    }

                    log.info("Reddit backfill complete ({} items)", dtos.size());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Step devtoStep() {
        return new StepBuilder("devtoStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    LocalDateTime end = LocalDateTime.now();
                    LocalDateTime start = end.minusMonths(6);

                    List<RawPostDto> dtos = devtoCrawler.fetchByDateRange(start, end);

                    for (RawPostDto dto : dtos) {
                        RawPostDto post = RawPostDto.builder()
                                .source(dto.source())
                                .title(dto.title())
                                .url(dto.url())
                                .createdAt(dto.createdAt())
                                .build();
                        indexDocument(post);
                    }

                    log.info("Dev.to backfill complete ({} items)", dtos.size());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
                .build();
    }

    /**
     * 공통 색인 메서드 (PUT 오류 방지용 id 조건 처리 포함)
     */
    private void indexDocument(RawPostDto post) {
        try {
            IndexRequest.Builder<RawPostDto> builder = new IndexRequest.Builder<RawPostDto>()
                    .index("raw-posts")
                    .document(post);

            if (post.url() != null && !post.url().isBlank()) {
                builder.id(post.url()); // URL을 문서 ID로 사용
            }

            esClient.index(builder.build());

        } catch (Exception e) {
            log.error("Failed to index post: url={} title={}", post.url(), post.title(), e);
            throw new RuntimeException(e);
        }
    }
}
