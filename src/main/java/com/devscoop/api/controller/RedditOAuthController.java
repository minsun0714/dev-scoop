package com.devscoop.api.controller;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.time.Duration;

@RestController
@RequestMapping("/oauth/reddit")
public class RedditOAuthController {

    // application-*.yml 혹은 환경변수에서 주입
    @Value("${reddit.client-id}")
    private String clientId;

    @Value("${reddit.secret}")
    private String clientSecret;

    // Reddit 앱에 등록한 redirect_uri와 "완전히 동일"해야 함
    @Value("${reddit.redirect-uri:https://api.dev-scoop.click/oauth/reddit/callback}")
    private String redirectUri;

    // 토큰 교환 후 사용자를 보낼 프론트 주소
    @Value("${app.login-success-redirect:https://dev-scoop.click/app?login=ok}")
    private String successRedirect;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @GetMapping("/callback")
    public ResponseEntity<Void> callback(@RequestParam String code,
                                         @RequestParam String state,
                                         HttpServletRequest request) {
        // 1) Reddit 토큰 교환
        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth(clientId, clientSecret);
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("grant_type", "authorization_code");
        form.add("code", code);
        form.add("redirect_uri", redirectUri); // ★ Reddit에 등록한 값과 완전 동일

        HttpEntity<MultiValueMap<String, String>> req = new HttpEntity<>(form, headers);

        ResponseEntity<String> tokenResp = restTemplate.postForEntity(
                "https://www.reddit.com/api/v1/access_token",
                req,
                String.class
        );

        if (!tokenResp.getStatusCode().is2xxSuccessful() || tokenResp.getBody() == null) {
            return redirect(successRedirect + "&error=oauth_failed");
        }

        try {
            // 2) 토큰 JSON 파싱
            RedditToken token = objectMapper.readValue(tokenResp.getBody(), RedditToken.class);

            // 3) 사용자 정보 조회 (선택: 닉네임 등 필요 시)
            HttpHeaders meHeaders = new HttpHeaders();
            meHeaders.setBearerAuth(token.accessToken());
            meHeaders.set("User-Agent", "DevScoop OAuth Client/1.0"); // Reddit 권장: UA 지정
            HttpEntity<Void> meReq = new HttpEntity<>(meHeaders);

            ResponseEntity<String> meResp = restTemplate.exchange(
                    "https://oauth.reddit.com/api/v1/me",
                    HttpMethod.GET,
                    meReq,
                    String.class
            );

            String meBody = meResp.getStatusCode().is2xxSuccessful() ? meResp.getBody() : null;
            // 필요하다면 meBody를 DTO로 파싱해서 username, id 등 꺼내 쓰기
            // ex) objectMapper.readTree(meBody).get("name").asText();

            // 4) 세션 저장 (시큐리티 없으므로 HttpSession 사용)
            HttpSession session = request.getSession(true);
            session.setAttribute("oauth.provider", "reddit");
            session.setAttribute("oauth.access_token", token.accessToken());
            session.setAttribute("oauth.refresh_token", token.refreshToken());
            session.setAttribute("oauth.expires_in", token.expiresIn());
            if (meBody != null) {
                session.setAttribute("oauth.me", meBody);
            }
            // 세션 타임아웃을 토큰 만료와 비슷하게 맞춤(옵션)
            if (token.expiresIn() != null && token.expiresIn() > 0) {
                int seconds = Math.min(token.expiresIn().intValue(), (int) Duration.ofHours(12).getSeconds());
                session.setMaxInactiveInterval(seconds);
            }

            // 5) (옵션) 액세스 토큰을 쿠키로 내려주고 싶다면 — HttpOnly 쿠키 권장
            ResponseCookie accessCookie = ResponseCookie.from("reddit_access_token", token.accessToken())
                    .httpOnly(true)           // JS에서 접근 불가 (보안)
                    .secure(true)             // HTTPS에서만
                    .sameSite("Lax")
                    .path("/")
                    .maxAge(token.expiresIn() != null ? token.expiresIn() : 3600)
                    .build();

            HttpHeaders redirectHeaders = new HttpHeaders();
            redirectHeaders.setLocation(URI.create(successRedirect));
            redirectHeaders.add(HttpHeaders.SET_COOKIE, accessCookie.toString());

            return new ResponseEntity<>(redirectHeaders, HttpStatus.FOUND); // 302

        } catch (Exception parseOrMeEx) {
            // 파싱/ME 호출 실패 시
            return redirect(successRedirect + "&error=oauth_parse_failed");
        }
    }

    private ResponseEntity<Void> redirect(String url) {
        HttpHeaders redirect = new HttpHeaders();
        redirect.setLocation(URI.create(url));
        return new ResponseEntity<>(redirect, HttpStatus.FOUND);
    }

    // Reddit 토큰 응답 DTO (필요 필드만)
    public record RedditToken(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("expires_in") Integer expiresIn,
            @JsonProperty("scope") String scope,
            @JsonProperty("refresh_token") String refreshToken
    ) {}
}
