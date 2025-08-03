package com.devscoop.api;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Arrays;

@SpringBootApplication
@EnableScheduling
@EnableJpaRepositories
public class DevscoopApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(DevscoopApplication.class, args);
		Environment env = ctx.getEnvironment();

		System.out.println("=== Spring Profiles ===");
		System.out.println("Active profiles: " + Arrays.toString(env.getActiveProfiles()));

		System.out.println("=== Redis Props ===");
		System.out.println("spring.redis.host = " + env.getProperty("spring.redis.host"));
		System.out.println("spring.redis.port = " + env.getProperty("spring.redis.port"));

		System.out.println("=== System Environment Variables (filtered) ===");
		System.getenv().forEach((k, v) -> {
			if (k.toLowerCase().contains("redis") || k.toLowerCase().contains("spring"))
				System.out.println(k + "=" + v);
		});

		// PropertySources 출력 (ConfigurableEnvironment 필요)
		System.out.println("=== Spring Property Sources ===");
		if (env instanceof ConfigurableEnvironment configurableEnv) {
			configurableEnv.getPropertySources().forEach(ps -> {
				System.out.println("Source: " + ps.getName());
			});
		}
	}
}
