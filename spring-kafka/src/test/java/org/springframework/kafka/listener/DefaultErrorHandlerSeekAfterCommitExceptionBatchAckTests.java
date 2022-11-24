/*
 * Copyright 2022 the original author or authors.
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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.9
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class DefaultErrorHandlerSeekAfterCommitExceptionBatchAckTests {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	/*
	 * Fail with commit exception - always seek.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void forceSeeksWithCommitException() throws Exception {
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.handleLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer, this.config.eh);
		inOrder.verify(this.consumer).assign(any(Collection.class));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(this.consumer).commitSync(any(), any());
		inOrder.verify(this.config.eh).handleRemaining(any(), any(), any(), any());
		verify(this.consumer, times(2)).seek(any(), anyLong());
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final List<String> contents = new ArrayList<>();

		final List<Integer> deliveries = new ArrayList<>();

		final CountDownLatch pollLatch = new CountDownLatch(4);

		final CountDownLatch deliveryLatch = new CountDownLatch(1);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		final CountDownLatch commitLatch = new CountDownLatch(1);

		final CountDownLatch handleLatch = new CountDownLatch(1);

		DefaultErrorHandler eh;

		int count;

		volatile org.apache.kafka.common.header.Header deliveryAttempt;

		@KafkaListener(groupId = "grp",
				topicPartitions = @org.springframework.kafka.annotation.TopicPartition(topic = "foo",
						partitions = "#{'0,1,2'.split(',')}"))
		public void foo(String in, @Header(KafkaHeaders.DELIVERY_ATTEMPT) int delivery) {
			this.contents.add(in);
			this.deliveries.add(delivery);
			this.deliveryLatch.countDown();
			if (++this.count == 4 || this.count == 5) { // part 1, offset 1, first and second times
				throw new RuntimeException("foo");
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
			final AtomicInteger which = new AtomicInteger();
			willAnswer(i -> {
				this.pollLatch.countDown();
				switch (which.getAndIncrement()) {
					case 0:
						return new ConsumerRecords(records1);
					default:
						try {
							Thread.sleep(50);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						return new ConsumerRecords(Collections.emptyMap());
				}
			}).given(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
			List<TopicPartition> paused = new ArrayList<>();
			AtomicBoolean first = new AtomicBoolean(true);
			willAnswer(i -> {
				this.commitLatch.countDown();
				if (first.getAndSet(false)) {
					throw new CommitFailedException();
				}
				return null;
			}).given(consumer).commitSync(anyMap(), any());
			willAnswer(i -> {
				this.closeLatch.countDown();
				return null;
			}).given(consumer).close();
			willAnswer(i -> {
				paused.addAll(i.getArgument(0));
				return null;
			}).given(consumer).pause(any());
			willAnswer(i -> {
				return new HashSet<>(paused);
			}).given(consumer).paused();
			willAnswer(i -> {
				paused.removeAll(i.getArgument(0));
				return null;
			}).given(consumer).resume(any());
			return consumer;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.getContainerProperties().setAckMode(AckMode.BATCH);
			factory.getContainerProperties().setDeliveryAttemptHeader(true);
			factory.setRecordInterceptor((record, consumer) -> {
				Config.this.deliveryAttempt = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
				return record;
			});
			this.eh = spy(new DefaultErrorHandler());
			this.eh.setSeekAfterError(false);
			willAnswer(inv -> {
				try {
					inv.callRealMethod();
				}
				finally {
					this.handleLatch.countDown();
				}
				return null;
			}).given(this.eh).handleRemaining(any(), any(), any(), any());
			factory.setCommonErrorHandler(eh);
			return factory;
		}

	}

}
