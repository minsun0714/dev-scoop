package com.devscoop.api.controller;

import com.devscoop.api.dto.KeywordRankingDto;
import com.devscoop.api.service.KeywordRankingService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class KeywordRankingController {

    private final KeywordRankingService keywordRankingService;

    @GetMapping("/ranking")
    public List<KeywordRankingDto> getKeywordRanking(
            @RequestParam(defaultValue = "all") String source,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return keywordRankingService.getKeywordRanking(source, limit);
    }
}
