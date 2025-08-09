//package com.devscoop.api.scheduler;
//
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.batch.core.Job;
//import org.springframework.batch.core.JobParameters;
//import org.springframework.batch.core.JobParametersBuilder;
//import org.springframework.batch.core.launch.JobLauncher;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import java.time.LocalDateTime;
//
//@Slf4j
//@Component
//@RequiredArgsConstructor
//public class BackfillJobScheduler {
//
//    private final JobLauncher jobLauncher;
//    private final Job backfillJob;
//
////    @Scheduled(cron = "0 3 6 * * *")
//    public void runBackfillJob() {
//        try {
//            JobParameters params = new JobParametersBuilder()
//                    .addString("requestedAt", LocalDateTime.now().toString()) // 재실행 구분자
//                    .toJobParameters();
//
//            log.info("Starting backfill job...");
//            jobLauncher.run(backfillJob, params);
//            log.info("Backfill job completed.");
//        } catch (Exception e) {
//            log.error("Failed to run backfill job", e);
//        }
//    }
//}
