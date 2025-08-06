package com.devscoop.api.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Value("${ELASTICSEARCH_HOST}")
    private String host;

    @Value("${ELASTICSEARCH_PORT}")
    private int port;

    @Bean
    public ElasticsearchClient elasticsearchClient() {
        // ES 클라이언트 생성
        RestClient restClient = RestClient.builder(
                new HttpHost(host, port)).build();

        // JavaTimeModule 등록
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // JSON Mapper + Transport 설정
        JacksonJsonpMapper mapper = new JacksonJsonpMapper(objectMapper);
        RestClientTransport transport = new RestClientTransport(restClient, mapper);

        return new ElasticsearchClient(transport);
    }
}
