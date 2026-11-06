package com.sporify.kafka_admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

public record UpdateTopicRequest(
		@Min(1) Integer partitions,
		@Min(1) Long retentionMs,
		@Pattern(regexp = "delete|compact|compact,delete", message = "cleanupPolicy debe ser delete, compact o compact,delete")
		String cleanupPolicy) {
}
