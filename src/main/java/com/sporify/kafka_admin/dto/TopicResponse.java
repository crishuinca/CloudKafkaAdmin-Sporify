package com.sporify.kafka_admin.dto;

public record TopicResponse(
		String name,
		boolean internal,
		int partitions,
		int replicas,
		String cleanupPolicy,
		Long retentionMs) {
}
