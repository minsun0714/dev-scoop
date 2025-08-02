package com.devscoop.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
@AllArgsConstructor
@Builder
public class ArticleSearchResponseDto {

    private final List<Map<String, Object>> content;
    private final long total;
    private final int page;
    private final int size;

    // 성공 응답
    public static ArticleSearchResponseDto of(List<Map<String, Object>> content, long total, int page, int size) {
        return ArticleSearchResponseDto.builder()
                .content(content)
                .total(total)
                .page(page)
                .size(size)
                .build();
    }

    // 빈 응답 (예외 등 실패 시)
    public static ArticleSearchResponseDto empty(int page, int size) {
        return ArticleSearchResponseDto.builder()
                .content(Collections.emptyList())
                .total(0)
                .page(page)
                .size(size)
                .build();
    }
}
