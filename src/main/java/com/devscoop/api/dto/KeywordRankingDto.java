package com.devscoop.api.dto;

import lombok.Builder;

@Builder
public record KeywordRankingDto(
        String keyword,
        Integer todayCount,
        Integer yesterdayCount,
        double score
) {
}