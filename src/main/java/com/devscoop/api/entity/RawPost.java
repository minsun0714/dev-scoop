package com.devscoop.api.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "raw_post")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RawPost {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 50)
    private String source;

    @Column(nullable = false, length = 2000)
    private String title;

    @Column(nullable = false, length = 2000)
    private String url;

    @Column(nullable = false, updatable = false)
    private LocalDateTime collectedAt;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    /**
     * Batch 처리에서만 사용되는 임시 키워드 목록.
     * DB 컬럼으로 매핑되지 않음.
     */
    @Transient
    private List<String> tempKeywords;

    @PrePersist
    public void prePersist() {
        this.collectedAt = LocalDateTime.now();
    }
}
