package com.devscoop.api.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/oauth/reddit")
public class RedditOAuthController {

    @Value("${reddit.client-id}")
    private String clientId;
    @Value("${reddit.secret}")
    private String clientSecret;
    @Value("${reddit.redirect-uri:https://api.dev-scoop.click/oauth/reddit/callback}")
    private String redirectUri;
    @Value("${app.login-success-redirect:https://dev-scoop.click/app?login=ok}")
    private String successRedirect;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 1) Reddit 동의 페이지로 리다이렉트 */
    @GetMapping("/authorize")
    public ResponseEntity<Void> authorize() {
        String authUrl = UriComponentsBuilder
                .fromHttpUrl("https://www.reddit.com/api/v1/authorize")
                .queryParam("client_id", clientId)
                .queryParam("response_type", "code")
                .queryParam("state", UUID.randomUUID().toString())
                .queryParam("redirect_uri", redirectUri)
                .queryParam("duration", "permanent")
                .queryParam("scope", "read,identity")
                .build().toUriString();

        HttpHeaders headers = new HttpHeaders();
        headers.setLocation(URI.create(authUrl));
        return new ResponseEntity<>(headers, HttpStatus.FOUND);
    }

    /** 2) Callback - 인가 코드 → 토큰 발급 */
    @GetMapping("/callback")
    public ResponseEntity<Void> callback(@RequestParam String code,
                                         @RequestParam String state,
                                         HttpServletRequest request) {
        try {
            // 토큰 요청
            HttpHeaders headers = new HttpHeaders();
            headers.setBasicAuth(clientId, clientSecret);
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            headers.set("User-Agent", "DevScoop OAuth Client/1.0");

            MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
            form.add("grant_type", "authorization_code");
            form.add("code", code);
            form.add("redirect_uri", redirectUri);

            HttpEntity<MultiValueMap<String, String>> req = new HttpEntity<>(form, headers);
            ResponseEntity<String> tokenResp = restTemplate.postForEntity(
                    "https://www.reddit.com/api/v1/access_token",
                    req,
                    String.class
            );

            if (!tokenResp.getStatusCode().is2xxSuccessful()) {
                return redirect(successRedirect + "&error=token_failed");
            }

            RedditToken token = objectMapper.readValue(tokenResp.getBody(), RedditToken.class);

            // Refresh Token 저장 (DB/Redis)
            request.getSession(true).setAttribute("reddit_refresh_token", token.refreshToken());

            return redirect(successRedirect);

        } catch (Exception e) {
            return redirect(successRedirect + "&error=callback_failed");
        }
    }

    private ResponseEntity<Void> redirect(String url) {
        HttpHeaders redirect = new HttpHeaders();
        redirect.setLocation(URI.create(url));
        return new ResponseEntity<>(redirect, HttpStatus.FOUND);
    }

    public record RedditToken(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("expires_in") Integer expiresIn,
            @JsonProperty("scope") String scope,
            @JsonProperty("refresh_token") String refreshToken
    ) {}
}
