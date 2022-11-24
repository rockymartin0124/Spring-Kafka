/*
 * Copyright 2017-2022 the original author or authors.
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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class ManualNackRecordTests {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;
	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	/*
	 * Deliver 6 records from three partitions, fail on the second record second
	 * partition, first attempt; verify partition 0,1 committed and a total of 7 records
	 * handled after seek.
	 */
	@SuppressWarnings({ "unchecked" })
	@Test
	public void discardRemainingRecordsFromPollAndSeek() throws Exception {
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.replayTime).isBetween(50L, 30_000L);
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer);
		inOrder.verify(this.consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		HashMap<TopicPartition, OffsetAndMetadata> commit1 = new HashMap<>();
		commit1.put(new TopicPartition("foo", 0), new OffsetAndMetadata(2L));
		commit1.put(new TopicPartition("foo", 1), new OffsetAndMetadata(1L));
		HashMap<TopicPartition, OffsetAndMetadata> commit2 = new HashMap<>();
		commit2.put(new TopicPartition("foo", 1), new OffsetAndMetadata(2L));
		commit2.put(new TopicPartition("foo", 2), new OffsetAndMetadata(2L));
		inOrder.verify(this.consumer).commitSync(commit1, Duration.ofSeconds(60));
		inOrder.verify(this.consumer).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verify(this.consumer).seek(new TopicPartition("foo", 2), 0L);
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(this.consumer).commitSync(commit2, Duration.ofSeconds(60));
		assertThat(this.config.count).isEqualTo(7);
		assertThat(this.config.contents.toArray()).isEqualTo(new String[]
				{ "foo", "bar", "baz", "qux", "qux", "fiz", "buz" });
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final List<String> contents = new ArrayList<>();

		final CountDownLatch pollLatch = new CountDownLatch(3);

		final CountDownLatch deliveryLatch = new CountDownLatch(7);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		final CountDownLatch commitLatch = new CountDownLatch(2);

		volatile int count;

		volatile long replayTime;

		@KafkaListener(topics = "foo", groupId = "grp")
		public void foo(String in, Acknowledgment ack) {
			this.contents.add(in);
			if (in.equals("qux")) {
				this.replayTime = System.currentTimeMillis() - this.replayTime;
			}
			this.deliveryLatch.countDown();
			if (++this.count == 4) { // part 1, offset 1, first time
				ack.nack(Duration.ofMillis(50));
			}
			else {
				ack.acknowledge();
			}
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer();
			given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public Consumer consumer() {
			final Consumer consumer = mock(Consumer.class);
			final TopicPartition topicPartition0 = new TopicPartition("foo", 0);
			final TopicPartition topicPartition1 = new TopicPartition("foo", 1);
			final TopicPartition topicPartition2 = new TopicPartition("foo", 2);
			willAnswer(i -> {
				((ConsumerRebalanceListener) i.getArgument(1)).onPartitionsAssigned(
						Collections.singletonList(topicPartition1));
				return null;
			}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
			Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
			records1.put(topicPartition0, Arrays.asList(
					new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "bar",
							new RecordHeaders(), Optional.empty())));
			records1.put(topicPartition1, Arrays.asList(
					new ConsumerRecord("foo", 1, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "baz",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord("foo", 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "qux",
							new RecordHeaders(), Optional.empty())));
			records1.put(topicPartition2, Arrays.asList(
					new ConsumerRecord("foo", 2, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "fiz",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord("foo", 2, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "buz",
							new RecordHeaders(), Optional.empty())));
			Map<TopicPartition, List<ConsumerRecord>> records2 = new LinkedHashMap<>(records1);
			records2.remove(topicPartition0);
			records2.put(topicPartition1, Arrays.asList(
					new ConsumerRecord("foo", 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "qux",
							new RecordHeaders(), Optional.empty())));
			final AtomicInteger which = new AtomicInteger();
			final AtomicBoolean paused = new AtomicBoolean();
			willAnswer(i -> {
				if (paused.get()) {
					Thread.sleep(10);
					return ConsumerRecords.empty();
				}
				this.pollLatch.countDown();
				switch (which.getAndIncrement()) {
					case 0:
						return new ConsumerRecords(records1);
					case 1:
						return new ConsumerRecords(records2);
					default:
						try {
							Thread.sleep(1000);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						return new ConsumerRecords(Collections.emptyMap());
				}
			}).given(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
			willAnswer(i -> {
				return Collections.emptySet();
			}).given(consumer).paused();
			willAnswer(i -> {
				paused.set(true);
				return null;
			}).given(consumer).pause(any());
			willAnswer(i -> {
				paused.set(false);
				return null;
			}).given(consumer).resume(any());
			willAnswer(i -> {
				this.commitLatch.countDown();
				return null;
			}).given(consumer).commitSync(anyMap(), any());
			willAnswer(i -> {
				this.closeLatch.countDown();
				return null;
			}).given(consumer).close();
			return consumer;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.getContainerProperties().setAckMode(AckMode.MANUAL);
			factory.getContainerProperties().setMissingTopicsFatal(false);
			factory.setRecordInterceptor(new RecordInterceptor() {

				@Override
				@Nullable
				@SuppressWarnings("rawtypes")
				public ConsumerRecord intercept(ConsumerRecord record, Consumer consumer) {
					return new ConsumerRecord(record.topic(), record.partition(), record.offset(), 0L,
							TimestampType.NO_TIMESTAMP_TYPE, 0, 0, record.key(), record.value(), record.headers(),
							Optional.empty());
				}

			});
			return factory;
		}

	}

}
