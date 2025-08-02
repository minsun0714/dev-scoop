package com.devscoop.api.extractor;

import com.openai.client.OpenAIClient;
import com.openai.models.ChatCompletion;
import com.openai.models.ChatCompletionCreateParams;
import com.openai.models.ChatModel;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
@RequiredArgsConstructor
public class TechKeywordExtractor {

    private final OpenAIClient openAIClient;

    public List<String> extractKeywords(String title) {
        ChatCompletionCreateParams params = ChatCompletionCreateParams.builder()
                .model(ChatModel.GPT_4O_MINI)
                .addUserMessage("다음 제목에서 기술 관련 키워드만 영어로 콤마로 구분해서 나열해줘. "
                        + "불필요한 단어는 제거하고 핵심 키워드만: " + title)
                .build();

        ChatCompletion completion = openAIClient.chat().completions().create(params);

        String content = completion.choices()
                .getFirst()
                .message()
                .content()
                .get();

        return Arrays.stream(content.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .toList();
    }
}
