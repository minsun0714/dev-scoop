package com.devscoop.api.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.devscoop.api.dto.RawPostDto;
import com.devscoop.api.extractor.TechKeywordExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class KeywordExtractJobConfig {

    private final ElasticsearchClient esClient;
    private final TechKeywordExtractor extractor;

    private static final int CHUNK_SIZE = 50;

    @Bean
    public Job keywordExtractJob(JobRepository jobRepository,
                                 PlatformTransactionManager transactionManager) {
        return new JobBuilder("keywordExtractJob", jobRepository)
                .start(keywordExtractStep(jobRepository, transactionManager))
                .build();
    }

    @Bean
    public Step keywordExtractStep(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager) {
        return new StepBuilder("keywordExtractStep", jobRepository)
                .<RawPostDto, RawPostDto>chunk(CHUNK_SIZE, transactionManager)
                .reader(elasticsearchReader())
                .processor(keywordProcessor())
                .writer(noopWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public ItemReader<RawPostDto> elasticsearchReader() {
        return new ItemReader<>() {
            private final Iterator<Hit<RawPostDto>> iterator;

            {
                try {
                    SearchRequest request = SearchRequest.of(s -> s
                            .index("raw-posts")
                            .size(1000));

                    SearchResponse<RawPostDto> response = esClient.search(request, RawPostDto.class);
                    iterator = response.hits().hits().iterator();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to initialize ES reader", e);
                }
            }

            @Override
            public RawPostDto read() {
                return iterator.hasNext() ? iterator.next().source() : null;
            }
        };
    }

    @Bean
    public ItemProcessor<RawPostDto, RawPostDto> keywordProcessor() {
        return post -> {
            List<String> keywords = extractor.extractKeywords(post.title());

            for (String keyword : keywords) {
                try {
                    upsertKeywordStat(keyword, post.source());
                } catch (Exception e) {
                    log.warn("[ES Upsert] Failed: keyword={} source={}", keyword, post.source(), e);
                }
            }

            return post;
        };
    }

    @Bean
    public ItemWriter<RawPostDto> noopWriter() {
        return items -> {};
    }

    private void upsertKeywordStat(String keyword, String source) throws Exception {
        var getRes = esClient.get(g -> g.index("keyword-stats").id(keyword), Map.class);

        Map<String, Object> doc = new HashMap<>();
        Map<String, Object> sources = new HashMap<>();
        int total = 1;

        if (getRes.found()) {
            Map<String, Object> existing = getRes.source();
            total = ((Number) existing.getOrDefault("total_count", 0)).intValue() + 1;
            Map<String, Object> existingSources = (Map<String, Object>) existing.getOrDefault("sources", new HashMap<>());
            int srcCount = ((Number) existingSources.getOrDefault(source, 0)).intValue() + 1;
            sources.putAll(existingSources);
            sources.put(source, srcCount);
        } else {
            sources.put(source, 1);
        }

        doc.put("keyword", keyword);
        doc.put("total_count", total);
        doc.put("sources", sources);
        doc.put("last_updated", LocalDateTime.now().toString());

        esClient.index(IndexRequest.of(r -> r
                .index("keyword-stats")
                .id(keyword)
                .document(doc)));
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-thread-");
        executor.setConcurrencyLimit(8);
        return executor;
    }
}
