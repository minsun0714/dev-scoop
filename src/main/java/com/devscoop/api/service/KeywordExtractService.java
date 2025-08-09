package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.devscoop.api.extractor.TechKeywordExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class KeywordExtractService {

    private final ElasticsearchClient es;
    private final TechKeywordExtractor extractor; // 키워드 추출기

    private static final String SRC_INDEX = "raw-posts";
    private static final String DEST_INDEX = "keyword-stats";

    public void extractKeywordsForHistory() {
        log.info("Starting keyword extraction (no Spring Batch)");
        String pitId = null;

        try {
            // 1) Point In Time 생성
            OpenPointInTimeResponse pit = es.openPointInTime(b -> b
                    .index(SRC_INDEX)
                    .keepAlive(Time.of(t -> t.time("5m"))));
            pitId = pit.id();

            List<FieldValue> searchAfter = null;
            final int size = 500;
            AtomicLong processed = new AtomicLong();

            while (true) {
                // 2) Search 요청 빌드
                String finalPitId = pitId;
                SearchRequest.Builder req = new SearchRequest.Builder()
                        .size(size)
                        .pit(p -> p.id(finalPitId).keepAlive(t -> t.time("5m")))
                        .sort(List.of(
                                SortOptions.of(o -> o.field(f -> f.field("createdAt").order(SortOrder.Asc))),
                                SortOptions.of(o -> o.field(f -> f.field("_shard_doc").order(SortOrder.Asc)))
                        ))
                        .query(q -> q.bool(b -> b
                                .must(m -> m.matchAll(ma -> ma))
                                .filter(f -> f.exists(e -> e.field("createdAt")))
                        ));

                if (searchAfter != null && !searchAfter.isEmpty()) {
                    req.searchAfter(searchAfter);
                }

                // 3) Elasticsearch 검색 실행
                SearchResponse<Map> res = es.search(req.build(), Map.class);
                if (res.hits().hits().isEmpty()) break;

                // 4) 결과 처리 → 키워드 추출 → Bulk 요청 준비
                List<BulkOperation> ops = new ArrayList<>();
                res.hits().hits().forEach(hit -> {
                    Map<String, Object> src = hit.source();
                    if (src == null) return;

                    String title  = Objects.toString(src.getOrDefault("title", ""), "");
                    String url    = Objects.toString(src.getOrDefault("url", ""), "");
                    String source = Objects.toString(src.getOrDefault("source", ""), "");
                    Instant createdAt = parseInstant(src.get("createdAt"));

                    List<String> keywords = extractor.extractKeywords(title);
                    if (keywords == null || keywords.isEmpty()) return;

                    Map<String, Object> doc = new HashMap<>();
                    doc.put("url", url);
                    doc.put("source", source);
                    doc.put("createdAt", createdAt == null ? null : createdAt.toString());
                    doc.put("keywords", keywords);

                    String id = (url == null || url.isBlank()) ? null : url;
                    ops.add(BulkOperation.of(o -> o.index(i -> i
                            .index(DEST_INDEX)
                            .id(id)
                            .document(doc)
                    )));
                });

                // 5) Bulk 저장
                if (!ops.isEmpty()) {
                    BulkResponse bulkRes = es.bulk(b -> b.operations(ops));
                    if (bulkRes.errors()) {
                        bulkRes.items().stream()
                                .filter(it -> it.error() != null)
                                .limit(10)
                                .forEach(it -> log.error("Bulk error: {}", it.error().reason()));
                    }
                }

                processed.addAndGet(res.hits().hits().size());

                // 6) 다음 페이지 커서 설정
                var lastHit = res.hits().hits().get(res.hits().hits().size() - 1);
                searchAfter = lastHit.sort();
                if (searchAfter == null || searchAfter.isEmpty()) break;
            }

            log.info("Keyword extraction completed. processed={}", processed.get());

        } catch (Exception e) {
            log.error("Keyword extraction failed", e);
        } finally {
            if (pitId != null) {
                try {
                    String finalPitId1 = pitId;
                    es.closePointInTime(c -> c.id(finalPitId1));
                } catch (Exception ignore) {}
            }
        }
    }

    private Instant parseInstant(Object v) {
        try {
            if (v == null) return null;
            if (v instanceof String s) return Instant.parse(s);
            if (v instanceof Long l)   return Instant.ofEpochMilli(l);
        } catch (Exception ignore) {}
        return LocalDateTime.now().toInstant(ZoneOffset.UTC);
    }
}
