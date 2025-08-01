package com.devscoop.api.crawler;

import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class GitHubTrendingCrawler {

    private static final String TRENDING_URL = "https://github.com/trending";
    private static final String USER_AGENT = "Mozilla/5.0 dev-scoop-crawler";

    public List<Element> fetchTrendingRepos() {
        try {
            Document doc = Jsoup.connect(TRENDING_URL)
                    .userAgent(USER_AGENT)
                    .get();
            return doc.select("article.Box-row h2 a");
        } catch (Exception e) {
            log.error("Failed to fetch GitHub trending repos", e);
            return Collections.emptyList();
        }
    }
}
