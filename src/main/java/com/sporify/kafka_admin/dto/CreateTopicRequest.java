package com.sporify.kafka_admin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record CreateTopicRequest(
		@NotBlank(message = "name es obligatorio") String name,
		@NotNull @Min(1) Integer partitions,
		@NotNull @Min(1) @Max(3) Integer replicas,
		@NotNull @Min(1) Long retentionMs,
		@NotBlank @Pattern(regexp = "delete|compact|compact,delete", message = "cleanupPolicy debe ser delete, compact o compact,delete")
		String cleanupPolicy) {
}
