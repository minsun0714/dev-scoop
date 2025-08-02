package com.devscoop.api.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(
        name = "keyword_freq",
        uniqueConstraints = @UniqueConstraint(columnNames = {"keyword", "site", "date"})
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeywordFreq {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String keyword;

    private String site;

    private LocalDate date;

    private int count;
}
