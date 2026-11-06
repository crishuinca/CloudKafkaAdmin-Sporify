package com.sporify.kafka_admin.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaAdminClientConfig {

	@Bean(destroyMethod = "close")
	AdminClient kafkaAdminClient(
			@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
		Map<String, Object> config = new HashMap<>();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
		config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
		return AdminClient.create(config);
	}
}
