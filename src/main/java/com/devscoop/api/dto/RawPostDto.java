package com.devscoop.api.dto;

import lombok.Builder;

import java.time.LocalDateTime;
import java.util.List;

@Builder
public record RawPostDto(
        String source,
        String title,
        String url,
        LocalDateTime createdAt,
        List<String> keywords
) {}
