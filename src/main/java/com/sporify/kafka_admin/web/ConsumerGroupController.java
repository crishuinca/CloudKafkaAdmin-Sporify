package com.sporify.kafka_admin.web;

import java.util.List;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.sporify.kafka_admin.dto.ConsumerGroupResponse;
import com.sporify.kafka_admin.service.KafkaClusterAdminService;

@RestController
@RequestMapping("/api/consumer-groups")
public class ConsumerGroupController {

	private final KafkaClusterAdminService admin;

	public ConsumerGroupController(KafkaClusterAdminService admin) {
		this.admin = admin;
	}

	@GetMapping
	@PreAuthorize("hasAuthority('SCOPE_Music.Read')")
	public List<ConsumerGroupResponse> list() {
		return admin.listGroups();
	}

	@GetMapping("/{groupId}")
	@PreAuthorize("hasAuthority('SCOPE_Music.Read')")
	public ConsumerGroupResponse get(@PathVariable String groupId) {
		return admin.getGroup(groupId);
	}
}
