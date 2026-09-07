package com.orderflow.search_indexing_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SearchIndexingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(SearchIndexingServiceApplication.class, args);
	}

}
