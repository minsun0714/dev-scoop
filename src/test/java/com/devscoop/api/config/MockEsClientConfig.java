package com.devscoop.api.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class MockEsClientConfig {
    @Bean
    @Primary
    public ElasticsearchClient elasticsearchClient() {
        return Mockito.mock(ElasticsearchClient.class); // 또는 ES 인터페이스를 구현한 Stub
    }
}