package com.sporify.kafka_admin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.sporify.kafka_admin.dto.CreateTopicRequest;
import com.sporify.kafka_admin.dto.UpdateTopicRequest;

@ExtendWith(MockitoExtension.class)
class KafkaClusterAdminServiceTest {

	@Mock
	private AdminClient admin;

	@Mock
	private CreateTopicsResult createTopicsResult;

	@Test
	void creaElTopicoConRetencionYCleanup() {
		when(admin.createTopics(any())).thenReturn(createTopicsResult);
		when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
		KafkaClusterAdminService service = new KafkaClusterAdminService(admin);

		service.createTopic(new CreateTopicRequest("sporify.demo", 3, 3, 3_600_000L, "delete"));

		@SuppressWarnings("unchecked")
		ArgumentCaptor<List<NewTopic>> captor = ArgumentCaptor.forClass(List.class);
		verify(admin).createTopics(captor.capture());
		NewTopic created = captor.getValue().get(0);
		assertEquals("sporify.demo", created.name());
		assertEquals(3, created.numPartitions());
		assertEquals((short) 3, created.replicationFactor());
		assertEquals("3600000", created.configs().get("retention.ms"));
		assertEquals("delete", created.configs().get("cleanup.policy"));
	}

	@Test
	void noReduceParticiones() {
		KafkaClusterAdminService service = new KafkaClusterAdminService(admin) {
			@Override
			public com.sporify.kafka_admin.dto.TopicResponse getTopic(String name) {
				return new com.sporify.kafka_admin.dto.TopicResponse(name, false, 3, 3, "delete", 1000L);
			}
		};

		IllegalArgumentException error = assertThrows(
				IllegalArgumentException.class,
				() -> service.updateTopic("sporify.demo", new UpdateTopicRequest(1, null, null)));
		assertEquals("Kafka no reduce particiones. Ahora tiene 3", error.getMessage());
	}
}
