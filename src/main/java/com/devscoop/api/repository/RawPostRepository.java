package com.devscoop.api.repository;

import com.devscoop.api.entity.RawPost;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RawPostRepository extends JpaRepository<RawPost, Long> {
}
