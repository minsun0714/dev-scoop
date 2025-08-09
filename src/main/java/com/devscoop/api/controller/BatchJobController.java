package com.devscoop.api.controller;

import com.devscoop.api.service.BackfillService;
import com.devscoop.api.service.KeywordExtractService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/job")
public class BatchJobController {

    private final KeywordExtractService keywordExtractService;
    private final BackfillService backfillJobService;

    @PostMapping("/keywords")
    public String extractHistory() {
        keywordExtractService.extractKeywordsForHistory();
        return "ok";
    }

    @PostMapping("/backfill")
    public String runBackfillJob() {
        backfillJobService.runBackfillJob();
        return "ok";
    }
}
