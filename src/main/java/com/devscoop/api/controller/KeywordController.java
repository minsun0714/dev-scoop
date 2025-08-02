package com.devscoop.api.controller;

import com.devscoop.api.service.KeywordExtractService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/keywords")
public class KeywordController {

    private final KeywordExtractService keywordExtractService;

    @PostMapping("/extract-history")
    public String extractHistory() {
        keywordExtractService.extractKeywordsForHistory();
        return "ok";
    }
}
