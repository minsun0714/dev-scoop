package com.devscoop.api.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RawPostDto {
    private String source;
    private String title;
    private String url;
    private LocalDateTime postedAt; // 원글 작성/발행 시간
}
