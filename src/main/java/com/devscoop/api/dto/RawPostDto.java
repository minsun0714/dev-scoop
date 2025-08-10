package com.devscoop.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@Builder
public record RawPostDto(
        String source,
        String title,
        String url,
        LocalDateTime createdAt, // 원문 작성일
        @JsonProperty("date_kst") String dateKst, // 수집일자 (YYYY-MM-DD)
        List<String> keywords
) {
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /** createdAt과 수집일자를 함께 넣는 팩토리 */
    public static RawPostDto of(String source, String title, String url,
                                LocalDateTime createdAt, List<String> keywords) {
        String dateKst = LocalDate.now(KST).toString(); // 수집일자
        return new RawPostDto(source, title, url, createdAt, dateKst, keywords);
    }
}
