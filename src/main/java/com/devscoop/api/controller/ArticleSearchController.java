package com.devscoop.api.controller;

import com.devscoop.api.dto.ArticleSearchResponseDto;
import com.devscoop.api.service.ArticleSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ArticleSearchController {

    private final ArticleSearchService searchService;

    @GetMapping("/search")
    public ArticleSearchResponseDto searchPosts(
            @RequestParam String keyword,
            @RequestParam(required = false) String source,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size
    ) {
        return searchService.search(keyword, source, page, size);
    }
}
