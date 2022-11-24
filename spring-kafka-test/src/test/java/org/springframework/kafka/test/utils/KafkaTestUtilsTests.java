/*
 * Copyright 2019-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * @author Gary Russell
 * @since 2.2.7
 *
 */
@EmbeddedKafka(topics = { "singleTopic1", "singleTopic2", "singleTopic3", "singleTopic4", "singleTopic5",
		"multiTopic1" })
public class KafkaTestUtilsTests {

	@Test
	void testGetSingleWithMoreThatOneTopic(EmbeddedKafkaBroker broker) {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
		producer.send(new ProducerRecord<>("singleTopic1", 0, 1, "foo"));
		producer.send(new ProducerRecord<>("singleTopic2", 0, 1, "foo"));
		producer.close();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("ktuTests1", "false", broker);
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		broker.consumeFromAllEmbeddedTopics(consumer);
		KafkaTestUtils.getSingleRecord(consumer, "singleTopic1");
		KafkaTestUtils.getSingleRecord(consumer, "singleTopic2");
		Map<TopicPartition, Long> endOffsets = KafkaTestUtils.getEndOffsets(consumer, "singleTopic1");
		assertThat(endOffsets).hasSize(2);
		assertThat(endOffsets.get(new TopicPartition("singleTopic1", 0))).isEqualTo(1L);
		assertThat(endOffsets.get(new TopicPartition("singleTopic1", 1))).isEqualTo(0L);
		consumer.close();
	}

	@Test
	void testGetSingleWithMoreThatOneTopicRecordNotThereYet(EmbeddedKafkaBroker broker) {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
		producer.send(new ProducerRecord<>("singleTopic4", 0, 1, "foo"));
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("ktuTests2", "false", broker);
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		broker.consumeFromEmbeddedTopics(consumer, "singleTopic4", "singleTopic5");
		long t1 = System.currentTimeMillis();
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() ->
			KafkaTestUtils.getSingleRecord(consumer, "singleTopic5", Duration.ofSeconds(2)));
		assertThat(System.currentTimeMillis() - t1).isGreaterThanOrEqualTo(2000L);
		producer.send(new ProducerRecord<>("singleTopic5", 1, "foo"));
		producer.close();
		KafkaTestUtils.getSingleRecord(consumer, "singleTopic4");
		KafkaTestUtils.getSingleRecord(consumer, "singleTopic5");
		Map<TopicPartition, Long> endOffsets = KafkaTestUtils.getEndOffsets(consumer, "singleTopic4", 0, 1);
		assertThat(endOffsets).hasSize(2);
		assertThat(endOffsets.get(new TopicPartition("singleTopic4", 0))).isEqualTo(1L);
		assertThat(endOffsets.get(new TopicPartition("singleTopic4", 1))).isEqualTo(0L);
		consumer.close();
	}

	@Test
	public void testGetOneRecord(EmbeddedKafkaBroker broker) throws Exception {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
		producer.send(new ProducerRecord<>("singleTopic3", 0, 1, "foo"));
		producer.close();
		ConsumerRecord<?, ?> oneRecord = KafkaTestUtils.getOneRecord(broker.getBrokersAsString(), "getOne",
				"singleTopic3", 0, false, true, Duration.ofSeconds(10));
		assertThat(oneRecord.value()).isEqualTo("foo");
		assertThat(KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(), "getOne", "singleTopic3", 0))
				.isNotNull()
				.extracting(omd -> omd.offset())
				.isEqualTo(1L);
		oneRecord = KafkaTestUtils.getOneRecord(broker.getBrokersAsString(), "getOne",
				"singleTopic3", 0, true, true, Duration.ofSeconds(10));
		assertThat(oneRecord.value()).isEqualTo("foo");
		assertThat(KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(), "getOne", "singleTopic3", 0))
				.isNotNull()
				.extracting(omd -> omd.offset())
				.isEqualTo(1L);
	}

	@Test
	public void testMultiMinRecords(EmbeddedKafkaBroker broker) throws Exception {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
		producer.send(new ProducerRecord<>("multiTopic1", 0, 1, "foo"));
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("ktuTests3", "false", broker);
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		broker.consumeFromAnEmbeddedTopic(consumer, "multiTopic1");
		new Thread(() -> {
			try {
				Thread.sleep(500);
				producer.send(new ProducerRecord<>("multiTopic1", 0, 1, "bar"));
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}).start();
		ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), 2);
		assertThat(records.count()).isEqualTo(2);
		producer.close();
		consumer.close();
	}

	@Test
	public void testGetCurrentOffsetWithAdminClient(EmbeddedKafkaBroker broker) throws Exception {
		Map<String, Object> adminClientProps = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
		try (AdminClient adminClient = AdminClient.create(adminClientProps); KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps)) {
			producer.send(new ProducerRecord<>("singleTopic3", 0, 1, "foo"));

			KafkaTestUtils.getOneRecord(broker.getBrokersAsString(), "testGetCurrentOffsetWithAdminClient",
					"singleTopic3", 0, false, true, Duration.ofSeconds(10));
			assertThat(KafkaTestUtils.getCurrentOffset(adminClient, "testGetCurrentOffsetWithAdminClient", "singleTopic3", 0))
					.isNotNull()
					.extracting(omd -> omd.offset())
					.isEqualTo(1L);
		}

	}

}
