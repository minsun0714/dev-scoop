package com.devscoop.api.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

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

    @Column(nullable = false)
    private LocalDateTime collectedAt;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    public void prePersist() {
        this.createdAt = LocalDateTime.now();
    }
}
