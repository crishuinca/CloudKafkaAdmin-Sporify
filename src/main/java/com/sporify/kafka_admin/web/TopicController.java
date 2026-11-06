package com.sporify.kafka_admin.web;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.sporify.kafka_admin.dto.AdminActionResponse;
import com.sporify.kafka_admin.dto.CreateTopicRequest;
import com.sporify.kafka_admin.dto.TopicResponse;
import com.sporify.kafka_admin.dto.UpdateTopicRequest;
import com.sporify.kafka_admin.service.KafkaClusterAdminService;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/topics")
public class TopicController {

	private final KafkaClusterAdminService admin;

	public TopicController(KafkaClusterAdminService admin) {
		this.admin = admin;
	}

	@GetMapping
	@PreAuthorize("hasAuthority('SCOPE_Music.Read')")
	public List<TopicResponse> list(@RequestParam(defaultValue = "false") boolean includeInternal) {
		return admin.listTopics(includeInternal);
	}

	@GetMapping("/{name}")
	@PreAuthorize("hasAuthority('SCOPE_Music.Read')")
	public TopicResponse get(@PathVariable String name) {
		return admin.getTopic(name);
	}

	@PostMapping
	@ResponseStatus(HttpStatus.CREATED)
	@PreAuthorize("hasAuthority('SCOPE_Music.Write')")
	public AdminActionResponse create(@Valid @RequestBody CreateTopicRequest request) {
		admin.createTopic(request);
		return new AdminActionResponse("created", "topic", request.name().trim());
	}

	@PatchMapping("/{name}")
	@PreAuthorize("hasAuthority('SCOPE_Music.Write')")
	public TopicResponse update(@PathVariable String name, @Valid @RequestBody UpdateTopicRequest request) {
		admin.updateTopic(name, request);
		return admin.getTopic(name);
	}

	@DeleteMapping("/{name}")
	@PreAuthorize("hasAuthority('SCOPE_Music.Write')")
	public AdminActionResponse delete(@PathVariable String name) {
		admin.deleteTopic(name);
		return new AdminActionResponse("deleted", "topic", name);
	}
}
