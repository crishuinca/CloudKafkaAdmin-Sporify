package com.sporify.kafka_admin.dto;

import java.util.List;

public record ConsumerGroupResponse(
		String groupId,
		String state,
		int members,
		List<String> assignments) {
}
