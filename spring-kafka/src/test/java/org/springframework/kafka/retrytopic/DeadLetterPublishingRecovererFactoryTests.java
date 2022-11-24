/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.core.NestedRuntimeException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.TimestampedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.condition.LogLevels;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
class DeadLetterPublishingRecovererFactoryTests {

	private final Clock clock = TestClockUtils.CLOCK;

	@Mock
	private DestinationTopicResolver destinationTopicResolver;

	private final String testTopic = "test-topic";

	private final String testRetryTopic = "test-topic-retry-0";

	private final Object key = new Object();

	private final Object value = new Object();

	private final ConsumerRecord<?, ?> consumerRecord = new ConsumerRecord<>(testTopic, 2, 0, key, value);

	@Mock
	private DestinationTopic destinationTopic;

	@Mock
	private KafkaOperations<?, ?> kafkaOperations;

	@Mock
	private CompletableFuture<?> completableFuture;

	@Captor
	private ArgumentCaptor<ProducerRecord> producerRecordCaptor;

	@Mock
	private Consumer<DeadLetterPublishingRecoverer> dlprCustomizer;

	private final long originalTimestamp = Instant.now(this.clock).toEpochMilli();

	private final byte[] originalTimestampBytes = BigInteger.valueOf(originalTimestamp).toByteArray();

	private final long nowTimestamp = Instant.now(this.clock).toEpochMilli();

	@Test
	void shouldSendMessage() {
		// setup
		TimestampedException e = new TimestampedException(new RuntimeException(), Instant.now(this.clock));
		long failureTimestamp = e.getTimestamp();
		given(destinationTopicResolver.resolveDestinationTopic("id", testTopic, 1, e, failureTimestamp))
				.willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopicResolver.getDestinationTopicByName("id", testRetryTopic)).willReturn(destinationTopic);
		given(destinationTopic.getDestinationDelay()).willReturn(1000L);
		willReturn(this.kafkaOperations).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations.send(any(ProducerRecord.class))).willReturn(completableFuture);
		this.consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampBytes);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(this.consumerRecord, e);

		// then
		then(kafkaOperations).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		assertThat(producerRecord.topic()).isEqualTo(testRetryTopic);
		assertThat(producerRecord.value()).isEqualTo(value);
		assertThat(producerRecord.key()).isEqualTo(key);
		assertThat(producerRecord.partition()).isEqualTo(2);

		// assert headers
		Header attemptsHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		assertThat(attemptsHeader).isNotNull();
		assertThat(ByteBuffer.wrap(attemptsHeader.value()).getInt()).isEqualTo(2);
		Header timestampHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP);
		assertThat(timestampHeader).isNotNull();
		assertThat(new BigInteger(timestampHeader.value()).longValue()).isEqualTo(failureTimestamp + 1000L);
	}

	@Test
	void shouldIncreaseAttemptsInLegacyHeader() {

		// setup
		RuntimeException e = new RuntimeException();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, key, value);
		consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, BigInteger.valueOf(127).toByteArray());
		consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, this.originalTimestampBytes);

		given(destinationTopicResolver.resolveDestinationTopic("id", testTopic, 127, e, originalTimestamp))
				.willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopicResolver.getDestinationTopicByName("id", testRetryTopic)).willReturn(destinationTopic);
		willReturn(kafkaOperations).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations.send(any(ProducerRecord.class))).willReturn(completableFuture);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(
				this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		then(kafkaOperations).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header attemptsHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		assertThat(attemptsHeader).isNotNull();
		assertThat(attemptsHeader.value().length).isEqualTo(Integer.BYTES); // handled a legacy one byte header ok
		assertThat(ByteBuffer.wrap(attemptsHeader.value()).getInt()).isEqualTo(128);
	}

	@Test
	void shouldIncreaseAttemptsInNewHeader() {

		// setup
		RuntimeException e = new RuntimeException();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, key, value);
		consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
				ByteBuffer.wrap(new byte[4]).putInt(127).array());
		consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, this.originalTimestampBytes);

		given(destinationTopicResolver.resolveDestinationTopic("id", testTopic, 127, e, originalTimestamp))
				.willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopicResolver.getDestinationTopicByName("id", testRetryTopic)).willReturn(destinationTopic);
		willReturn(kafkaOperations).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations.send(any(ProducerRecord.class))).willReturn(completableFuture);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(
				this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		then(kafkaOperations).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header attemptsHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		assertThat(attemptsHeader).isNotNull();
		assertThat(attemptsHeader.value().length).isEqualTo(Integer.BYTES);
		assertThat(ByteBuffer.wrap(attemptsHeader.value()).getInt()).isEqualTo(128);
	}

	@Test
	void shouldAddOriginalTimestampHeaderAndCustom() {

		// setup
		RuntimeException e = new RuntimeException();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, originalTimestamp,
				TimestampType.CREATE_TIME, -1, -1, key, value, new RecordHeaders(), Optional.empty());

		given(destinationTopicResolver.resolveDestinationTopic("id", testTopic, 1, e, originalTimestamp))
				.willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopicResolver.getDestinationTopicByName("id", testRetryTopic)).willReturn(destinationTopic);
		willReturn(this.kafkaOperations).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations.send(any(ProducerRecord.class))).willReturn(completableFuture);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(
				this.destinationTopicResolver);
		factory.setHeadersFunction(
				(rec, ex) -> new RecordHeaders(new RecordHeader[] { new RecordHeader("foo", "bar".getBytes()) }));

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		then(kafkaOperations).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header originalTimestampHeader = producerRecord.headers()
				.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP);
		assertThat(originalTimestampHeader).isNotNull();
		assertThat(new BigInteger(originalTimestampHeader.value()).longValue()).isEqualTo(this.nowTimestamp);
		assertThat(producerRecord.headers().lastHeader("foo")).extracting("value").isEqualTo("bar".getBytes());
	}

	@Test
	void shouldNotReplaceOriginalTimestampHeader() {

		// setup
		RuntimeException e = new RuntimeException();
		long timestamp = LocalDateTime.now(this.clock).toInstant(ZoneOffset.UTC).minusMillis(5000).toEpochMilli();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, timestamp,
				TimestampType.CREATE_TIME, -1, -1, key, value, new RecordHeaders(), Optional.empty());

		given(destinationTopicResolver.resolveDestinationTopic("id", testTopic, 1, e, timestamp))
				.willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testRetryTopic);
		given(destinationTopicResolver.getDestinationTopicByName("id", testRetryTopic)).willReturn(destinationTopic);
		willReturn(this.kafkaOperations).given(destinationTopic).getKafkaOperations();
		given(kafkaOperations.send(any(ProducerRecord.class))).willReturn(completableFuture);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(
				this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		then(kafkaOperations).should(times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header originalTimestampHeader = producerRecord.headers()
				.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP);
		assertThat(originalTimestampHeader).isNotNull();
		assertThat(new BigInteger(originalTimestampHeader.value()).longValue()).isEqualTo(timestamp);
	}

	@Test
	void shouldNotSendMessageIfNoOpsDestination() {
		// setup
		RuntimeException e = new RuntimeException();
		given(destinationTopicResolver.resolveDestinationTopic("id", testTopic, 1, e, originalTimestamp))
				.willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(true);
		this.consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampBytes);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(this.consumerRecord, e);

		// then
		then(kafkaOperations).should(times(0)).send(any(ProducerRecord.class));
	}

	@Test
	void shouldThrowIfKafkaBackoffException() {
		// setup
		RuntimeException e = new KafkaBackoffException("KBEx", new TopicPartition("", 0), "test-listener-id",
				this.nowTimestamp);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(
				this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		assertThatExceptionOfType(NestedRuntimeException.class)
				.isThrownBy(() -> deadLetterPublishingRecoverer.accept(this.consumerRecord, e));

		// then
		then(kafkaOperations).should(times(0)).send(any(ProducerRecord.class));
	}

	@Test
	void shouldCallDLPRCustomizer() {

		// given
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);
		factory.setDeadLetterPublishingRecovererCustomizer(dlprCustomizer);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");

		// then
		then(dlprCustomizer).should(times(1)).accept(deadLetterPublishingRecoverer);
	}

	@Test
	void shouldSendMessageEvenIfCircularFatal() {
		// setup
		TimestampedException e = new TimestampedException(new IllegalStateException(), Instant.now(this.clock));
		long failureTimestamp = e.getTimestamp();
		given(destinationTopicResolver.resolveDestinationTopic("id", testTopic, 1, e, failureTimestamp))
				.willReturn(destinationTopic);
		given(destinationTopic.isNoOpsTopic()).willReturn(false);
		given(destinationTopic.getDestinationName()).willReturn(testTopic);
		given(this.destinationTopicResolver.getDestinationTopicByName("id", testTopic)).willReturn(destinationTopic);
		willReturn(kafkaOperations).given(destinationTopic).getKafkaOperations();
		this.consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ORIGINAL_TIMESTAMP, originalTimestampBytes);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(
				this.destinationTopicResolver);
		factory.addNotRetryableException(IllegalStateException.class);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.setFailIfSendResultIsError(false);
		deadLetterPublishingRecoverer.accept(this.consumerRecord, e);

		// then
		then(kafkaOperations).should(times(1)).send(any(ProducerRecord.class));
	}

	@LogLevels(classes = DeadLetterPublishingRecovererFactory.class, level = "info")
	@Test
	void shouldNeverLogIfSet() {

		// setup
		RuntimeException retryException = new RuntimeException("Test exception");
		DestinationTopicResolver resolver = mock(DestinationTopicResolver.class);
		setupTopic(false, false, resolver, retryException);

		RuntimeException dltException = new RuntimeException("Test exception");
		setupTopic(false, true, resolver, dltException);

		RuntimeException noOpsException = new RuntimeException("Test exception");
		setupTopic(true, false, resolver, noOpsException);

		ConsumerRecord recordMock = mock(ConsumerRecord.class);
		Headers headersMock = mock(Headers.class);
		given(recordMock.topic()).willReturn("testTopic");
		given(recordMock.headers()).willReturn(headersMock);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(resolver) {
			@Override
			protected TopicPartition resolveTopicPartition(ConsumerRecord<?, ?> cr, DestinationTopic nextDestination) {
				return null;
			}
		};
		factory.neverLogListenerException();

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(recordMock, retryException);
		then(headersMock).should(never()).lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, dltException);
		then(headersMock).should(never()).lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, noOpsException);
		then(headersMock).should(never()).lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
	}

	@LogLevels(classes = DeadLetterPublishingRecovererFactory.class, level = "info")
	@Test
	void shouldAlwaysLogIfSet() {

		// setup
		RuntimeException retryException = new RuntimeException("Test exception");
		DestinationTopicResolver resolver = mock(DestinationTopicResolver.class);
		setupTopic(false, false, resolver, retryException);

		RuntimeException dltException = new RuntimeException("Test exception");
		setupTopic(false, true, resolver, dltException);

		RuntimeException noOpsException = new RuntimeException("Test exception");
		setupTopic(true, false, resolver, noOpsException);

		ConsumerRecord recordMock = mock(ConsumerRecord.class);
		Headers headersMock = mock(Headers.class);
		given(recordMock.topic()).willReturn("testTopic");
		given(recordMock.headers()).willReturn(headersMock);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(resolver) {
			@Override
			protected TopicPartition resolveTopicPartition(ConsumerRecord<?, ?> cr, DestinationTopic nextDestination) {
				return null;
			}
		};
		factory.alwaysLogListenerException();

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(recordMock, retryException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, dltException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, noOpsException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
	}

	@LogLevels(classes = DeadLetterPublishingRecovererFactory.class, level = "info")
	@Test
	void shouldLogAfterExhaustedByDefault() {

		// setup
		RuntimeException retryException = new RuntimeException("Test exception");
		DestinationTopicResolver resolver = mock(DestinationTopicResolver.class);
		setupTopic(false, false, resolver, retryException);

		RuntimeException dltException = new RuntimeException("Test exception");
		setupTopic(false, true, resolver, dltException);

		RuntimeException noOpsException = new RuntimeException("Test exception");
		setupTopic(true, false, resolver, noOpsException);

		ConsumerRecord recordMock = mock(ConsumerRecord.class);
		Headers headersMock = mock(Headers.class);
		given(recordMock.topic()).willReturn("testTopic");
		given(recordMock.headers()).willReturn(headersMock);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(resolver) {
			@Override
			protected TopicPartition resolveTopicPartition(ConsumerRecord<?, ?> cr, DestinationTopic nextDestination) {
				return null;
			}
		};

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(recordMock, retryException);
		then(headersMock).should(never()).lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, dltException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, noOpsException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
	}

	@LogLevels(classes = DeadLetterPublishingRecovererFactory.class, level = "debug")
	@Test
	public void shouldAlwaysLogAtDebugLevel() {

		// setup
		RuntimeException retryException = new RuntimeException("Test exception");
		DestinationTopicResolver resolver = mock(DestinationTopicResolver.class);
		setupTopic(false, false, resolver, retryException);

		RuntimeException dltException = new RuntimeException("Test exception");
		setupTopic(false, true, resolver, dltException);

		RuntimeException noOpsException = new RuntimeException("Test exception");
		setupTopic(true, false, resolver, noOpsException);

		ConsumerRecord recordMock = mock(ConsumerRecord.class);
		Headers headersMock = mock(Headers.class);
		given(recordMock.topic()).willReturn("testTopic");
		given(recordMock.headers()).willReturn(headersMock);

		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(resolver) {
			@Override
			protected TopicPartition resolveTopicPartition(ConsumerRecord<?, ?> cr, DestinationTopic nextDestination) {
				return null;
			}
		};
		factory.neverLogListenerException();

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create("id");
		deadLetterPublishingRecoverer.accept(recordMock, retryException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, dltException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
		Mockito.reset(headersMock);

		deadLetterPublishingRecoverer.accept(recordMock, noOpsException);
		then(headersMock).should().lastHeader(KafkaHeaders.ORIGINAL_TOPIC);
	}

	private void setupTopic(boolean noOps, boolean dlt, DestinationTopicResolver resolver, RuntimeException noOpsException) {
		DestinationTopic noOpsDestination = mock(DestinationTopic.class);
		given(noOpsDestination.isNoOpsTopic()).willReturn(noOps);
		given(noOpsDestination.isDltTopic()).willReturn(dlt);
		given(resolver.resolveDestinationTopic(anyString(), anyString(), anyInt(), eq(noOpsException),
				anyLong())).willReturn(noOpsDestination);
	}

}
