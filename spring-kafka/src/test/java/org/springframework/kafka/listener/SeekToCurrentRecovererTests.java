/*
 * Copyright 2018-2021 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
@EmbeddedKafka(topics = {SeekToCurrentRecovererTests.topic1, SeekToCurrentRecovererTests.topic1DLT })
public class SeekToCurrentRecovererTests {

	public static final String topic1 = "seekTopic1";

	public static final String topic1DLT = "seekTopic1.FOO";

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	public void testMaxFailures() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("seekTestMaxFailures", "false", embeddedKafka);
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props, null,
				new ErrorHandlingDeserializer<>(new JsonDeserializer<>(String.class)));
		ContainerProperties containerProps = new ContainerProperties(topic1);
		containerProps.setPollTimeout(10_000);

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		DefaultKafkaProducerFactory<Object, Object> pf = new DefaultKafkaProducerFactory<>(senderProps);
		final KafkaTemplate<Object, Object> template = new KafkaTemplate<>(pf);
		Serializer<?> byteArraySerializer = new ByteArraySerializer();
		@SuppressWarnings("unchecked")
		DefaultKafkaProducerFactory<Object, Object> dltPf =
				new DefaultKafkaProducerFactory<Object, Object>(senderProps, null, (Serializer<Object>) byteArraySerializer);
		final CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<String> data = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> {
			data.set(message.value());
			if (message.offset() == 0) {
				throw new ListenerExecutionFailedException("fail for max failures");
			}
			latch.countDown();
		});

		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testSeekMaxFailures");
		final CountDownLatch recoverLatch = new CountDownLatch(1);
		final AtomicReference<String> failedGroupId = new AtomicReference<>();
		Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
		templates.put(String.class, template);
		templates.put(byte[].class, new KafkaTemplate<>(dltPf));
		DeadLetterPublishingRecoverer recoverer =
				new DeadLetterPublishingRecoverer(templates,
						(r, e) -> new TopicPartition(topic1DLT, r.partition())) {

			@Override
			public void accept(ConsumerRecord<?, ?> record, @Nullable Consumer<?, ?> consumer, Exception exception) {
				super.accept(record, consumer, exception);
				if (exception instanceof ListenerExecutionFailedException) {
					failedGroupId.set(((ListenerExecutionFailedException) exception).getGroupId());
				}
				recoverLatch.countDown();
			}

		};
		DefaultErrorHandler errorHandler = spy(new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 2)));
		container.setCommonErrorHandler(errorHandler);
		final CountDownLatch stopLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(e -> {
			if (e instanceof ConsumerStoppedEvent) {
				stopLatch.countDown();
			}
		});
		container.start();

		template.setDefaultTopic(topic1);
		template.sendDefault(0, 0, "\"foo\"");
		template.sendDefault(0, 0, "\"bar\"");
		assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(data.get()).isEqualTo("bar");
		assertThat(recoverLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(failedGroupId.get()).isEqualTo("seekTestMaxFailures");

		props.put(ConsumerConfig.GROUP_ID_CONFIG, "seekTestMaxFailures.dlt");
		DefaultKafkaConsumerFactory<Integer, String> dltcf = new DefaultKafkaConsumerFactory<>(props);
		Consumer<Integer, String> consumer = dltcf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topic1DLT);
		ConsumerRecord<Integer, String> dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic1DLT);
		assertThat(dltRecord.value()).isEqualTo("foo");
		template.sendDefault(0, 0, "junkJson");
		dltRecord = KafkaTestUtils.getSingleRecord(consumer, topic1DLT);
		assertThat(dltRecord.value()).isEqualTo("junkJson");
		container.stop();
		pf.destroy();
		dltPf.destroy();
		consumer.close();
		assertThat(stopLatch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(errorHandler, times(4)).handleRemaining(any(), any(), any(), any());
		verify(errorHandler).clearThreadState();
	}

	@Test
	public void seekToCurrentErrorHandlerRecovers() {
		@SuppressWarnings("unchecked")
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		DefaultErrorHandler eh = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 1));
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 0, 1, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
				eh.handleRemaining(new RuntimeException(), records, consumer, null));
		verify(consumer).seek(new TopicPartition("foo", 0),  0L);
		verifyNoMoreInteractions(consumer);
		eh.handleRemaining(new RuntimeException(), records, consumer, null);
		verify(consumer).seek(new TopicPartition("foo", 0),  1L);
		verifyNoMoreInteractions(consumer);
		verify(recoverer).accept(eq(records.get(0)), any());
	}

	@Test
	public void seekToCurrentErrorHandlerRecovererFailsBackOffReset() {
		@SuppressWarnings("unchecked")
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		AtomicBoolean fail = new AtomicBoolean(true);
		willAnswer(incovation -> {
			if (fail.getAndSet(false)) {
				throw new RuntimeException("recovery failed");
			}
			return null;
		}).given(recoverer).accept(any(), any());
		DefaultErrorHandler eh = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 1));
		AtomicInteger failedDeliveryAttempt = new AtomicInteger();
		AtomicReference<Exception> recoveryFailureEx = new AtomicReference<>();
		AtomicBoolean isRecovered = new AtomicBoolean();
		eh.setRetryListeners(new RetryListener() {

			@Override
			public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
				failedDeliveryAttempt.set(deliveryAttempt);
			}

			@Override
			public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
				isRecovered.set(true);
			}

			@Override
			public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
				recoveryFailureEx.set(failure);
			}

		});
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 0, 1, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(
				() -> eh.handleRemaining(new RuntimeException(), records, consumer, null));
		verify(consumer).seek(new TopicPartition("foo", 0),  0L);
		verifyNoMoreInteractions(consumer);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(
				() -> eh.handleRemaining(new RuntimeException(), records, consumer, null));
		verify(consumer, times(2)).seek(new TopicPartition("foo", 0),  0L);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(
				() -> eh.handleRemaining(new RuntimeException(), records, consumer, null));
		verify(consumer, times(3)).seek(new TopicPartition("foo", 0),  0L);
		eh.handleRemaining(new RuntimeException(), records, consumer, null);
		verify(consumer, times(3)).seek(new TopicPartition("foo", 0),  0L);
		verify(consumer).seek(new TopicPartition("foo", 0),  1L);
		verifyNoMoreInteractions(consumer);
		verify(recoverer, times(2)).accept(eq(records.get(0)), any());
		assertThat(failedDeliveryAttempt.get()).isEqualTo(2);
		assertThat(recoveryFailureEx.get())
				.isInstanceOf(RuntimeException.class)
				.extracting(ex -> ex.getMessage())
				.isEqualTo("recovery failed");
		assertThat(isRecovered.get()).isTrue();
	}

	@Test
	public void seekToCurrentErrorHandlerRecovererFailsBackOffNotReset() {
		@SuppressWarnings("unchecked")
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		AtomicBoolean fail = new AtomicBoolean(true);
		willAnswer(incovation -> {
			if (fail.getAndSet(false)) {
				throw new RuntimeException("recovery failed");
			}
			return null;
		}).given(recoverer).accept(any(), any());
		DefaultErrorHandler eh = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 1));
		eh.setResetStateOnRecoveryFailure(false);
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 0, 1, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(
				() -> eh.handleRemaining(new RuntimeException(), records, consumer, null));
		verify(consumer).seek(new TopicPartition("foo", 0),  0L);
		verifyNoMoreInteractions(consumer);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(
				() -> eh.handleRemaining(new RuntimeException(), records, consumer, null));
		verify(consumer, times(2)).seek(new TopicPartition("foo", 0),  0L);
		eh.handleRemaining(new RuntimeException(), records, consumer, null); // immediate re-attempt recovery
		verify(consumer, times(2)).seek(new TopicPartition("foo", 0),  0L);
		verify(consumer).seek(new TopicPartition("foo", 0),  1L);
		verifyNoMoreInteractions(consumer);
		verify(recoverer, times(2)).accept(eq(records.get(0)), any());
	}

	@Test
	public void seekToCurrentErrorHandlerRecoversManualAcksAsync() {
		seekToCurrentErrorHandlerRecoversManualAcks(false);
	}

	@Test
	public void seekToCurrentErrorHandlerRecoversManualAcksSync() {
		seekToCurrentErrorHandlerRecoversManualAcks(true);
	}

	private void seekToCurrentErrorHandlerRecoversManualAcks(boolean syncCommits) {
		@SuppressWarnings("unchecked")
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		DefaultErrorHandler eh = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, 1));
		eh.setCommitRecovered(true);
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 1, 0, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("foo");
		properties.setAckMode(AckMode.MANUAL_IMMEDIATE);
		properties.setSyncCommits(syncCommits);
		properties.setSyncCommitTimeout(Duration.ofSeconds(42));
		OffsetCommitCallback commitCallback = (offsets, ex) -> { };
		properties.setCommitCallback(commitCallback);
		given(container.getContainerProperties()).willReturn(properties);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
			eh.handleRemaining(new RuntimeException(), records, consumer, container));
		verify(consumer).seek(new TopicPartition("foo", 0),  0L);
		verify(consumer).seek(new TopicPartition("foo", 1),  0L);
		verifyNoMoreInteractions(consumer);
		eh.handleRemaining(new RuntimeException(), records, consumer, container);
		verify(consumer, times(2)).seek(new TopicPartition("foo", 1),  0L);
		if (syncCommits) {
			verify(consumer)
					.commitSync(Collections.singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(1L)),
							Duration.ofSeconds(42));
		}
		else {
			verify(consumer)
					.commitAsync(
							Collections.singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(1L)),
							commitCallback);
		}
		verifyNoMoreInteractions(consumer);
		verify(recoverer).accept(eq(records.get(0)), any());
	}

	@Test
	public void testNeverRecover() {
		@SuppressWarnings("unchecked")
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		DefaultErrorHandler eh = new DefaultErrorHandler(recoverer, new FixedBackOff(0L, Long.MAX_VALUE));
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		records.add(new ConsumerRecord<>("foo", 0, 0, null, "foo"));
		records.add(new ConsumerRecord<>("foo", 0, 1, null, "bar"));
		Consumer<?, ?> consumer = mock(Consumer.class);
		for (int i = 0; i < 20; i++) {
			assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
				eh.handleRemaining(new RuntimeException(), records, consumer, null));
		}
		verify(consumer, times(20)).seek(new TopicPartition("foo", 0),  0L);
		verifyNoMoreInteractions(consumer);
		verify(recoverer, never()).accept(any(), any());
	}

}
