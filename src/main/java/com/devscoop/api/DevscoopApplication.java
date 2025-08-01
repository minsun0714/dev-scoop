package com.devscoop.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DevscoopApplication {

	public static void main(String[] args) {
		SpringApplication.run(DevscoopApplication.class, args);
	}

}
