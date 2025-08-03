package com.devscoop.api.repository;

import com.devscoop.api.entity.RawPost;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface RawPostRepository extends JpaRepository<RawPost, Long> {

    @Query(value = """
        SELECT AVG(daily_count) AS mean, STDDEV_POP(daily_count) AS std
        FROM (
          SELECT DATE(created_at) AS dt, COUNT(*) AS daily_count
          FROM raw_post
          WHERE source = :source
            AND title LIKE %:keyword%
          GROUP BY DATE(created_at)
        ) t
        """, nativeQuery = true)
    Double[] calcMeanAndStd(String source, String keyword);
}
