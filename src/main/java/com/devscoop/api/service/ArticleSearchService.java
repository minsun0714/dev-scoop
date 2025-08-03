package com.devscoop.api.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.devscoop.api.dto.ArticleSearchResponseDto;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ArticleSearchService {

    private final ElasticsearchClient esClient;
    private final ObjectMapper objectMapper;

    public ArticleSearchResponseDto search(String keyword, String source, int page, int size) {

        try {
            // Bool query (must: keyword match, filter: source term)
            BoolQuery.Builder boolQuery = new BoolQuery.Builder()
                    .must(m -> m.multiMatch(mm -> mm
                            .fields("title^2", "keywords")  // title에 가중치
                            .query(keyword)
                    ));

            if (source != null && !source.isBlank()) {
                boolQuery.filter(f -> f.term(TermQuery.of(t -> t
                        .field("source")
                        .value(source)
                )));
            }

            int from = (page - 1) * size;

            SearchRequest request = new SearchRequest.Builder()
                    .index("raw-posts")
                    .query(q -> q.bool(boolQuery.build()))
                    .sort(s -> s.field(f -> f.field("createdAt").order(SortOrder.Desc)))
                    .from(from)
                    .size(size)
                    .build();

            // 검색 실행
            SearchResponse<Map> response = esClient.search(request, Map.class);

            // 검색 결과 변환
            List<Map<String, Object>> results = response.hits().hits().stream()
                    .map(hit -> objectMapper.convertValue(hit.source(),
                            new TypeReference<Map<String, Object>>() {}))
                    .toList();

            long total = response.hits().total() != null ? response.hits().total().value() : 0L;

            return ArticleSearchResponseDto.of(results, total, page, size);

        } catch (Exception e) {
            log.error("Failed to search in Elasticsearch", e);
            return ArticleSearchResponseDto.empty(page, size);
        }
    }
}
