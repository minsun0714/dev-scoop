package com.devscoop.api.config;

import com.devscoop.api.crawler.*;
import com.devscoop.api.dto.RawPostDto;
import com.devscoop.api.entity.RawPost;
import com.devscoop.api.reader.IteratorItemReader;
import com.devscoop.api.repository.RawPostRepository;
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
    private final RawPostRepository repository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    /**
     * 전체 백필 Job
     */
    @Bean
    public Job backfillJob() {
        return new JobBuilder("backfillJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(hackerNewsStep())
                .next(redditStep())
                .next(devtoStep())
                .build();
    }

    /**
     * HackerNews 6개월치 데이터를 직접 DB에 저장
     */
    @Bean
    public Step hackerNewsStep() {
        LocalDateTime end = LocalDateTime.now();
        LocalDateTime start = end.minusMonths(6);

        List<RawPostDto> dtos = hackerNewsCrawler.fetchByDateRange(start, end)
                .stream()
                .filter(dto -> !dto.getPostedAt().isBefore(start))
                .toList();

        log.info(">>> HackerNews Step will process {} items", dtos.size());

        return new StepBuilder("hackerNewsStep", jobRepository)
                .<RawPostDto, RawPost>chunk(1000, transactionManager)
                .reader(new IteratorItemReader<>(dtos.iterator()))
                .processor(dto -> RawPost.builder()
                        .source(dto.getSource())
                        .title(dto.getTitle())
                        .url(dto.getUrl())
                        .createdAt(dto.getPostedAt())
                        .build())
                .writer(items -> {
                    log.info("Writer called with {} items", items.size());
                    for (RawPost post : items) {
                        try {
                            repository.saveAndFlush(post);  // 개별 flush
                        } catch (Exception e) {
                            log.error("Failed to save post: titleLen={} urlLen={} url={}",
                                    post.getTitle().length(),
                                    post.getUrl().length(),
                                    post.getUrl(), e);
                            throw e;
                        }
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

                    repository.saveAll(
                            dtos.stream()
                                    .map(dto -> RawPost.builder()
                                            .source(dto.getSource())
                                            .title(dto.getTitle())
                                            .url(dto.getUrl())
                                            .createdAt(dto.getPostedAt())
                                            .build())
                                    .toList()
                    );
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

                    repository.saveAll(
                            dtos.stream()
                                    .map(dto -> RawPost.builder()
                                            .source(dto.getSource())
                                            .title(dto.getTitle())
                                            .url(dto.getUrl())
                                            .createdAt(dto.getPostedAt())
                                            .build())
                                    .toList()
                    );
                    log.info("Dev.to backfill complete ({} items)", dtos.size());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .allowStartIfComplete(true)
                .build();
    }
}