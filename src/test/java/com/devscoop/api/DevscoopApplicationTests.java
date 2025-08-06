package com.devscoop.api;

import com.devscoop.api.config.MockEsClientConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@Import(MockEsClientConfig.class)
class DevscoopApplicationTests {

	@Test
	void contextLoads() {
	}

}
