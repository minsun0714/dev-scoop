package com.devscoop.api.config;

import com.devscoop.api.entity.RawPost;
import com.devscoop.api.extractor.TechKeywordExtractor;
import com.devscoop.api.repository.KeywordFreqRepository;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class KeywordExtractJobConfig {

    private final EntityManagerFactory emf;
    private final TechKeywordExtractor extractor;
    private final KeywordFreqRepository keywordFreqRepository;

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
                .<RawPost, RawPost>chunk(CHUNK_SIZE, transactionManager)
                .reader(rawPostReader())
                .processor(keywordProcessor())
                .writer(keywordWriter())
                .taskExecutor(taskExecutor()) // 병렬 실행 추가
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        // SimpleAsyncTaskExecutor는 스레드를 빠르게 만들어주는 가벼운 Executor
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-thread-");
        executor.setConcurrencyLimit(8); // 동시에 최대 8개까지
        return executor;
    }

    @Bean
    public JpaPagingItemReader<RawPost> rawPostReader() {
        return new JpaPagingItemReaderBuilder<RawPost>()
                .name("rawPostReader")
                .entityManagerFactory(emf)
                .queryString("SELECT r FROM RawPost r ORDER BY r.id ASC")
                .pageSize(CHUNK_SIZE)
                .build();
    }

    @Bean
    public ItemProcessor<RawPost, RawPost> keywordProcessor() {
        return post -> {
            List<String> keywords = extractor.extractKeywords(post.getTitle());
            post.setTempKeywords(keywords);
            return post;
        };
    }

    @Bean
    public ItemWriter<RawPost> keywordWriter() {
        return items -> {
            for (RawPost post : items) {
                List<String> keywords = post.getTempKeywords();
                if (keywords == null) continue;
                for (String keyword : keywords) {
                    try {
                        keywordFreqRepository.upsert(
                                keyword,
                                post.getSource(),
                                post.getCreatedAt().toLocalDate()
                        );
                    } catch (Exception e) {
                        // 에러 무시하고 진행
                    }
                }
            }
        };
    }
}
