package com.devscoop.api.repository;

import com.devscoop.api.entity.KeywordFreq;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;

@Repository
public interface KeywordFreqRepository extends JpaRepository<KeywordFreq, Long> {

    @Modifying
    @Transactional
    @Query(
            value = """
        INSERT INTO keyword_freq(keyword, site, date, count)
        VALUES (:keyword, :site, :date, 1)
        ON DUPLICATE KEY UPDATE count = count + 1
        """,
            nativeQuery = true
    )
    void upsert(@Param("keyword") String keyword,
                @Param("site") String site,
                @Param("date") LocalDate date);
}
