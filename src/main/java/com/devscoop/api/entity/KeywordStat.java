package com.devscoop.api.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(
        name = "keyword_stats",
        uniqueConstraints = @UniqueConstraint(columnNames = {"source", "keyword"})
)
public class KeywordStat {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 50)
    private String source; // "all" 또는 reddit ...

    @Column(nullable = false, length = 255)
    private String keyword;

    @Column(nullable = false)
    private double mean;

    @Column(nullable = false)
    private double std;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public static KeywordStat of(String source, String keyword, double mean, double std) {
        KeywordStat ks = new KeywordStat();
        ks.source = source;
        ks.keyword = keyword;
        ks.mean = mean;
        ks.std = std;
        ks.updatedAt = LocalDateTime.now();
        return ks;
    }

    public void updateStats(double mean, double std) {
        this.mean = mean;
        this.std = std;
        this.updatedAt = LocalDateTime.now();
    }
}
