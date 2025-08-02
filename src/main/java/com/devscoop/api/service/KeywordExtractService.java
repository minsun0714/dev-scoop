package com.devscoop.api.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeywordExtractService {

    private final JobLauncher jobLauncher;
    private final Job keywordExtractJob;

    public void extractKeywordsForHistory() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("requestedAt", LocalDateTime.now().toString()) // 재실행 구분자
                    .toJobParameters();

            log.info("Starting keywordExtractJob...");
            jobLauncher.run(keywordExtractJob, params);
            log.info("keywordExtractJob completed.");

        } catch (Exception e) {
            log.error("Failed to run keywordExtractJob", e);
        }
    }
}
