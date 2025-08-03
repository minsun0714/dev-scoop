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
public class BackfillJobService {

    private final JobLauncher jobLauncher;
    private final Job backfillJob;

    public String runBackfillJob() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("requestedAt", LocalDateTime.now().toString())
                    .toJobParameters();

            log.info("Starting backfill job...");
            jobLauncher.run(backfillJob, params);
            log.info("Backfill job completed.");
            return "Backfill job started successfully";
        } catch (Exception e) {
            log.error("Failed to run backfill job", e);
            return "Failed to start backfill job: " + e.getMessage();
        }
    }
}
