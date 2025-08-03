package com.devscoop.api.repository;

import com.devscoop.api.entity.KeywordStat;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface KeywordStatRepository extends JpaRepository<KeywordStat, Long> {

    Optional<KeywordStat> findBySourceAndKeyword(String source, String keyword);
}
