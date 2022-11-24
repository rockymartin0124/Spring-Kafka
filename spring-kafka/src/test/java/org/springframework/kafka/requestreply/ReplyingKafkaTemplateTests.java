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

package org.springframework.kafka.requestreply;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.adapter.ReplyHeadersConfigurer;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.1.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(partitions = 5, topics = { ReplyingKafkaTemplateTests.A_REPLY, ReplyingKafkaTemplateTests.A_REQUEST,
		ReplyingKafkaTemplateTests.B_REPLY, ReplyingKafkaTemplateTests.B_REQUEST,
		ReplyingKafkaTemplateTests.C_REPLY, ReplyingKafkaTemplateTests.C_REQUEST,
		ReplyingKafkaTemplateTests.D_REPLY, ReplyingKafkaTemplateTests.D_REQUEST,
		ReplyingKafkaTemplateTests.E_REPLY, ReplyingKafkaTemplateTests.E_REQUEST,
		ReplyingKafkaTemplateTests.F_REPLY, ReplyingKafkaTemplateTests.F_REQUEST,
		ReplyingKafkaTemplateTests.G_REPLY, ReplyingKafkaTemplateTests.G_REQUEST,
		ReplyingKafkaTemplateTests.H_REPLY, ReplyingKafkaTemplateTests.H_REQUEST,
		ReplyingKafkaTemplateTests.I_REPLY, ReplyingKafkaTemplateTests.I_REQUEST,
		ReplyingKafkaTemplateTests.J_REPLY, ReplyingKafkaTemplateTests.J_REQUEST,
		ReplyingKafkaTemplateTests.K_REPLY, ReplyingKafkaTemplateTests.K_REQUEST,
		ReplyingKafkaTemplateTests.L_REPLY, ReplyingKafkaTemplateTests.L_REQUEST })
public class ReplyingKafkaTemplateTests {

	public static final String A_REPLY = "aReply";

	public static final String A_REQUEST = "aRequest";

	public static final String B_REPLY = "bReply";

	public static final String B_REQUEST = "bRequest";

	public static final String C_REPLY = "cReply";

	public static final String C_REQUEST = "cRequest";

	public static final String D_REPLY = "dReply";

	public static final String D_REQUEST = "dRequest";

	public static final String E_REPLY = "eReply";

	public static final String E_REQUEST = "eRequest";

	public static final String F_REPLY = "fReply";

	public static final String F_REQUEST = "fRequest";

	public static final String G_REPLY = "gReply";

	public static final String G_REQUEST = "gRequest";

	public static final String H_REPLY = "hReply";

	public static final String H_REQUEST = "hRequest";

	public static final String I_REPLY = "iReply";

	public static final String I_REQUEST = "iRequest";

	public static final String J_REPLY = "jReply";

	public static final String J_REQUEST = "jRequest";

	public static final String K_REPLY = "kReply";

	public static final String K_REQUEST = "kRequest";

	public static final String L_REPLY = "lReply";

	public static final String L_REQUEST = "lRequest";

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	public String testName;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@BeforeEach
	public void captureTestName(TestInfo info) {
		this.testName = info.getTestMethod().get().getName();
	}

	@Test
	public void testGood() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			Headers headers = new RecordHeaders();
			headers.add("baz", "buz".getBytes());
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, null, null, null, "foo", headers);
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
			Map<String, Object> receivedHeaders = new HashMap<>();
			new DefaultKafkaHeaderMapper().toHeaders(consumerRecord.headers(), receivedHeaders);
			assertThat(receivedHeaders).containsKey("baz");
			assertThat(receivedHeaders).hasSize(2);
			assertThat(this.registry.getListenerContainer(A_REQUEST).getContainerProperties().isMissingTopicsFatal())
					.isFalse();
			ProducerRecord<Integer, String> record2 =
					new ProducerRecord<>(A_REQUEST, null, null, null, "slow", headers);
			assertThatExceptionOfType(ExecutionException.class)
					.isThrownBy(() -> template.sendAndReceive(record2, Duration.ZERO).get(10, TimeUnit.SECONDS))
					.withCauseExactlyInstanceOf(KafkaReplyTimeoutException.class);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodWithMessage() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setMessageConverter(new StringJsonMessageConverter());
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			RequestReplyMessageFuture<Integer, String> fut = template
					.sendAndReceive(MessageBuilder.withPayload("foo")
							.setHeader(KafkaHeaders.TOPIC, A_REQUEST)
							.build());
			fut.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			Message<?> reply = fut.get(30, TimeUnit.SECONDS);
			assertThat(reply.getPayload()).isEqualTo("FOO");

			RequestReplyTypedMessageFuture<Integer, String, Foo> fooFut = template.sendAndReceive(
						MessageBuilder.withPayload("getAFoo")
						.setHeader(KafkaHeaders.TOPIC, A_REQUEST)
						.build(),
					new ParameterizedTypeReference<Foo>() {
					});
			fooFut.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			@SuppressWarnings("unchecked")
			Message<Foo> typed = fooFut.get(30, TimeUnit.SECONDS);
			assertThat(typed.getPayload().getBar()).isEqualTo("baz");

			RequestReplyTypedMessageFuture<Integer, String, List<Foo>> foosFut = template.sendAndReceive(
						MessageBuilder.withPayload("getFoos")
						.setHeader(KafkaHeaders.TOPIC, A_REQUEST)
						.build(),
					new ParameterizedTypeReference<List<Foo>>() {
					});
			foosFut.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			@SuppressWarnings("unchecked")
			Message<List<Foo>> typedFoos = foosFut.get(30, TimeUnit.SECONDS);
			List<Foo> foos = typedFoos.getPayload();
			assertThat(foos).hasSize(2);
			assertThat(foos.get(0).getBar()).isEqualTo("baz");
			assertThat(foos.get(1).getBar()).isEqualTo("qux");
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	void userDefinedException() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(L_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			template.setReplyErrorChecker(record -> {
				org.apache.kafka.common.header.Header error = record.headers().lastHeader("serverSentAnError");
				if (error != null) {
					return new IllegalStateException(new String(error.value()));
				}
				else {
					return null;
				}
			});
			ProducerRecord<Integer, String> record = new ProducerRecord<>(L_REQUEST, null, null, null, "foo", null);
			assertThatExceptionOfType(ExecutionException.class)
					.isThrownBy(() -> template.sendAndReceive(record).get(10, TimeUnit.SECONDS))
					.withCauseExactlyInstanceOf(IllegalStateException.class)
					.withMessageContaining("user error");
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	void testConsumerRecord() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(K_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			Headers headers = new RecordHeaders();
			ProducerRecord<Integer, String> record = new ProducerRecord<>(K_REQUEST, null, null, null, "foo", headers);
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testBadDeserialize() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(J_REPLY, true);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			Headers headers = new RecordHeaders();
			headers.add("baz", "buz".getBytes());
			ProducerRecord<Integer, String> record = new ProducerRecord<>(J_REQUEST, null, null, null, "foo", headers);
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			assertThatExceptionOfType(ExecutionException.class).isThrownBy(() -> future.get(10, TimeUnit.SECONDS))
					.withCauseExactlyInstanceOf(DeserializationException.class);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testMultiListenerMessageReturn() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(C_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			ProducerRecord<Integer, String> record = new ProducerRecord<>(C_REQUEST, "foo");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, C_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testHandlerReturn() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(I_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			ProducerRecord<Integer, String> record = new ProducerRecord<>(I_REQUEST, "foo");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, I_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testMessageReturnNoHeadersProvidedByListener() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(H_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			ProducerRecord<Integer, String> record = new ProducerRecord<>(H_REQUEST, "foo");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, H_REPLY.getBytes()));
			byte[] four = new byte[4];
			four[3] = 4;
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_PARTITION, four));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("FOO");
			assertThat(consumerRecord.partition()).isEqualTo(4);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodDefaultReplyHeaders() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(
				new TopicPartitionOffset(A_REPLY, 3));
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, "bar");
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("BAR");
			assertThat(consumerRecord.partition()).isEqualTo(3);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodDefaultReplyHeadersStringCorrelation() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(
				new TopicPartitionOffset(A_REPLY, 3));
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			template.setBinaryCorrelation(false);
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, "bar");
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("BAR");
			assertThat(consumerRecord.partition()).isEqualTo(3);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodSamePartition() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(A_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			ProducerRecord<Integer, String> record = new ProducerRecord<>(A_REQUEST, 2, null, "baz");
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, A_REPLY.getBytes()));
			record.headers()
					.add(new RecordHeader(KafkaHeaders.REPLY_PARTITION, new byte[] { 0, 0, 0, 2 }));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("BAZ");
			assertThat(consumerRecord.partition()).isEqualTo(2);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testGoodWithSimpleMapper() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(B_REPLY);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			Headers headers = new RecordHeaders();
			headers.add("baz", "buz".getBytes());
			ProducerRecord<Integer, String> record = new ProducerRecord<>(B_REQUEST, null, null, null, "qux", headers);
			record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, B_REPLY.getBytes()));
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("qUX");
			Map<String, Object> receivedHeaders = new HashMap<>();
			new DefaultKafkaHeaderMapper().toHeaders(consumerRecord.headers(), receivedHeaders);
			assertThat(receivedHeaders).containsKey("qux");
			assertThat(receivedHeaders).doesNotContainKey("baz");
			assertThat(receivedHeaders).hasSize(2);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testAggregateNormal() throws Exception {
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(D_REPLY, 0), 3, new AtomicInteger());
		try {
			template.setCorrelationHeaderName("customCorrelation");
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			ProducerRecord<Integer, String> record = new ProducerRecord<>(D_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, Collection<ConsumerRecord<Integer, String>>> consumerRecord =
					future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value().size()).isEqualTo(3);
			Iterator<ConsumerRecord<Integer, String>> iterator = consumerRecord.value().iterator();
			String value1 = iterator.next().value();
			assertThat(value1).isIn("fOO", "FOO", "Foo");
			String value2 = iterator.next().value();
			assertThat(value2).isIn("fOO", "FOO", "Foo");
			assertThat(value2).isNotSameAs(value1);
			String value3 = iterator.next().value();
			assertThat(value3).isIn("fOO", "FOO", "Foo");
			assertThat(value3).isNotSameAs(value1);
			assertThat(value3).isNotSameAs(value2);
			assertThat(consumerRecord.topic()).isEqualTo(AggregatingReplyingKafkaTemplate.AGGREGATED_RESULTS_TOPIC);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testAggregateNormalStringCorrelation() throws Exception {
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(D_REPLY, 0), 3, new AtomicInteger());
		try {
			template.setCorrelationHeaderName("customCorrelation");
			template.setBinaryCorrelation(false);
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			ProducerRecord<Integer, String> record = new ProducerRecord<>(D_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, Collection<ConsumerRecord<Integer, String>>> consumerRecord =
					future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value().size()).isEqualTo(3);
			Iterator<ConsumerRecord<Integer, String>> iterator = consumerRecord.value().iterator();
			String value1 = iterator.next().value();
			assertThat(value1).isIn("fOO", "FOO", "Foo");
			String value2 = iterator.next().value();
			assertThat(value2).isIn("fOO", "FOO", "Foo");
			assertThat(value2).isNotSameAs(value1);
			String value3 = iterator.next().value();
			assertThat(value3).isIn("fOO", "FOO", "Foo");
			assertThat(value3).isNotSameAs(value1);
			assertThat(value3).isNotSameAs(value2);
			assertThat(consumerRecord.topic()).isEqualTo(AggregatingReplyingKafkaTemplate.AGGREGATED_RESULTS_TOPIC);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	@Disabled("time sensitive")
	public void testAggregateTimeout() throws Exception {
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(E_REPLY, 0), 4, new AtomicInteger());
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(5));
			template.setCorrelationHeaderName("customCorrelation");
			ProducerRecord<Integer, String> record = new ProducerRecord<>(E_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			try {
				future.get(30, TimeUnit.SECONDS);
				fail("Expected Exception");
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			}
			catch (ExecutionException e) {
				assertThat(e)
					.hasCauseExactlyInstanceOf(KafkaReplyTimeoutException.class)
					.hasMessageContaining("Reply timed out");
			}
			Thread.sleep(10_000);
			assertThat(KafkaTestUtils.getPropertyValue(template, "futures", Map.class)).isEmpty();
			assertThat(KafkaTestUtils.getPropertyValue(template, "pending", Map.class)).isEmpty();
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testAggregateTimeoutPartial() throws Exception {
		AtomicInteger releaseCount = new AtomicInteger();
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(F_REPLY, 0), 4, releaseCount);
		template.setReturnPartialOnTimeout(true);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(5));
			template.setCorrelationHeaderName("customCorrelation");
			ProducerRecord<Integer, String> record = new ProducerRecord<>(F_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, Collection<ConsumerRecord<Integer, String>>> consumerRecord =
					future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value().size()).isEqualTo(3);
			Iterator<ConsumerRecord<Integer, String>> iterator = consumerRecord.value().iterator();
			String value1 = iterator.next().value();
			assertThat(value1).isIn("fOO", "FOO", "Foo");
			String value2 = iterator.next().value();
			assertThat(value2).isIn("fOO", "FOO", "Foo");
			assertThat(value2).isNotSameAs(value1);
			String value3 = iterator.next().value();
			assertThat(value3).isIn("fOO", "FOO", "Foo");
			assertThat(value3).isNotSameAs(value1);
			assertThat(value3).isNotSameAs(value2);
			assertThat(consumerRecord.topic())
					.isEqualTo(AggregatingReplyingKafkaTemplate.PARTIAL_RESULTS_AFTER_TIMEOUT_TOPIC);
			assertThat(releaseCount.get()).isEqualTo(4);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@Test
	public void testAggregateTimeoutPartialStringCorrelation() throws Exception {
		AtomicInteger releaseCount = new AtomicInteger();
		AggregatingReplyingKafkaTemplate<Integer, String, String> template = aggregatingTemplate(
				new TopicPartitionOffset(F_REPLY, 0), 4, releaseCount);
		template.setReturnPartialOnTimeout(true);
		template.setBinaryCorrelation(false);
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(5));
			template.setCorrelationHeaderName("customCorrelation");
			ProducerRecord<Integer, String> record = new ProducerRecord<>(F_REQUEST, null, null, null, "foo");
			RequestReplyFuture<Integer, String, Collection<ConsumerRecord<Integer, String>>> future =
					template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, Collection<ConsumerRecord<Integer, String>>> consumerRecord =
					future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value().size()).isEqualTo(3);
			Iterator<ConsumerRecord<Integer, String>> iterator = consumerRecord.value().iterator();
			String value1 = iterator.next().value();
			assertThat(value1).isIn("fOO", "FOO", "Foo");
			String value2 = iterator.next().value();
			assertThat(value2).isIn("fOO", "FOO", "Foo");
			assertThat(value2).isNotSameAs(value1);
			String value3 = iterator.next().value();
			assertThat(value3).isIn("fOO", "FOO", "Foo");
			assertThat(value3).isNotSameAs(value1);
			assertThat(value3).isNotSameAs(value2);
			assertThat(consumerRecord.topic())
					.isEqualTo(AggregatingReplyingKafkaTemplate.PARTIAL_RESULTS_AFTER_TIMEOUT_TOPIC);
			assertThat(releaseCount.get()).isEqualTo(4);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testAggregateOrphansNotStored() throws Exception {
		GenericMessageListenerContainer container = mock(GenericMessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("two");
		properties.setAckMode(AckMode.MANUAL);
		given(container.getContainerProperties()).willReturn(properties);
		ProducerFactory pf = mock(ProducerFactory.class);
		Producer producer = mock(Producer.class);
		given(pf.createProducer()).willReturn(producer);
		AtomicReference<byte[]> correlation = new AtomicReference<>();
		willAnswer(invocation -> {
			ProducerRecord rec = invocation.getArgument(0);
			correlation.set(rec.headers().lastHeader(KafkaHeaders.CORRELATION_ID).value());
			return new CompletableFuture<>();
		}).given(producer).send(any(), any());
		AggregatingReplyingKafkaTemplate template = new AggregatingReplyingKafkaTemplate(pf, container,
				(list, timeout) -> true);
		template.setDefaultReplyTimeout(Duration.ofSeconds(30));
		template.start();
		List<ConsumerRecord> records = new ArrayList<>();
		ConsumerRecord record = new ConsumerRecord("two", 0, 0L, null, "test1");
		RequestReplyFuture future = template.sendAndReceive(new ProducerRecord("one", null, "test"));
		record.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlation.get()));
		records.add(record);
		Consumer consumer = mock(Consumer.class);
		template.onMessage(records, consumer);
		assertThat(future.get(10, TimeUnit.SECONDS)).isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(template, "pending", Map.class)).hasSize(0);
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		offsets.put(new TopicPartition("two", 0), new OffsetAndMetadata(1));
		verify(consumer).commitSync(offsets, Duration.ofSeconds(30));
		// simulate redelivery after completion
		template.onMessage(records, consumer);
		assertThat(KafkaTestUtils.getPropertyValue(template, "pending", Map.class)).hasSize(0);
		template.stop();
		template.destroy();
	}

	public ReplyingKafkaTemplate<Integer, String, String> createTemplate(String topic) throws Exception {
		return createTemplate(topic, false);
	}

	public ReplyingKafkaTemplate<Integer, String, String> createTemplate(String topic, boolean badDeser)
			throws Exception {

		ContainerProperties containerProperties = new ContainerProperties(topic);
		final CountDownLatch latch = new CountDownLatch(1);
		containerProperties.setConsumerRebalanceListener(new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// no op
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				latch.countDown();
			}

		});
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.testName, "false", embeddedKafka);
		if (badDeser) {
			consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
			consumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, BadDeser.class);
		}
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProperties);
		container.setBeanName(this.testName);
		ReplyingKafkaTemplate<Integer, String, String> template =
				new ReplyingKafkaTemplate<>(this.config.pf(), container);
		template.setSharedReplyTopic(true);
		template.start();
		assertThat(template.waitForAssignment(Duration.ofSeconds(10))).isTrue();
		assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(template.getAssignedReplyTopicPartitions()).hasSize(5);
		assertThat(template.getAssignedReplyTopicPartitions().iterator().next().topic()).isEqualTo(topic);
		return template;
	}

	public ReplyingKafkaTemplate<Integer, String, String> createTemplate(TopicPartitionOffset topic)
			throws InterruptedException {

		ContainerProperties containerProperties = new ContainerProperties(topic);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.testName, "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProperties);
		container.setBeanName(this.testName);
		ReplyingKafkaTemplate<Integer, String, String> template = new ReplyingKafkaTemplate<>(this.config.pf(),
				container);
		template.setSharedReplyTopic(true);
		template.start();
		assertThat(template.waitForAssignment(Duration.ofSeconds(10))).isTrue();
		assertThat(template.getAssignedReplyTopicPartitions()).hasSize(1);
		assertThat(template.getAssignedReplyTopicPartitions().iterator().next().topic()).isEqualTo(topic.getTopic());
		return template;
	}

	public AggregatingReplyingKafkaTemplate<Integer, String, String> aggregatingTemplate(
			TopicPartitionOffset topic, int releaseSize, AtomicInteger releaseCount) {

		ContainerProperties containerProperties = new ContainerProperties(topic);
		containerProperties.setAckMode(AckMode.MANUAL_IMMEDIATE);
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.testName, "false", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, Collection<ConsumerRecord<Integer, String>>> cf =
				new DefaultKafkaConsumerFactory<>(consumerProps);
		KafkaMessageListenerContainer<Integer, Collection<ConsumerRecord<Integer, String>>> container =
				new KafkaMessageListenerContainer<>(cf, containerProperties);
		container.setBeanName(this.testName);
		AggregatingReplyingKafkaTemplate<Integer, String, String> template =
				new AggregatingReplyingKafkaTemplate<>(this.config.pf(), container,
						(list, timeout) -> {
							releaseCount.incrementAndGet();
							return list.size() == releaseSize || timeout;
						});
		template.setSharedReplyTopic(true);
		template.start();
		assertThat(template.getAssignedReplyTopicPartitions()).hasSize(1);
		assertThat(template.getAssignedReplyTopicPartitions().iterator().next().topic()).isEqualTo(topic.getTopic());
		return template;
	}

	@Test
	public void withCustomHeaders() throws Exception {
		ReplyingKafkaTemplate<Integer, String, String> template = createTemplate(new TopicPartitionOffset(G_REPLY, 1));
		template.setCorrelationHeaderName("custom.correlation.id");
		template.setReplyTopicHeaderName("custom.reply.to");
		template.setReplyPartitionHeaderName("custom.reply.partition");
		try {
			template.setDefaultReplyTimeout(Duration.ofSeconds(30));
			Headers headers = new RecordHeaders();
			ProducerRecord<Integer, String> record = new ProducerRecord<>(G_REQUEST, null, null, null, "foo", headers);
			RequestReplyFuture<Integer, String, String> future = template.sendAndReceive(record);
			future.getSendFuture().get(10, TimeUnit.SECONDS); // send ok
			ConsumerRecord<Integer, String> consumerRecord = future.get(30, TimeUnit.SECONDS);
			assertThat(consumerRecord.value()).isEqualTo("fooWithCustomHeaders");
			assertThat(consumerRecord.partition()).isEqualTo(1);
		}
		finally {
			template.stop();
			template.destroy();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void nullDuration() throws Exception {
		ProducerFactory pf = mock(ProducerFactory.class);
		Producer producer = mock(Producer.class);
		willAnswer(invocation -> {
			Callback callback = invocation.getArgument(1);
			CompletableFuture<Object> future = new CompletableFuture<>();
			future.complete("done");
			callback.onCompletion(new RecordMetadata(new TopicPartition("foo", 0), 0L, 0, 0L, 0, 0), null);
			return future;
		}).given(producer).send(any(), any());
		given(pf.createProducer()).willReturn(producer);
		GenericMessageListenerContainer container = mock(GenericMessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("two");
		given(container.getContainerProperties()).willReturn(properties);
		ReplyingKafkaTemplate template = new ReplyingKafkaTemplate(pf, container);
		template.start();
		Message<?> msg = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(KafkaHeaders.TOPIC, "foo")
				.build();
		// was NPE here
		template.sendAndReceive(new ProducerRecord("foo", 0, "bar", "baz"), null).getSendFuture().get();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void requestTimeoutWithMessage() throws Exception {
		ProducerFactory pf = mock(ProducerFactory.class);
		Producer producer = mock(Producer.class);
		willAnswer(invocation -> {
			return new CompletableFuture<>();
		}).given(producer).send(any(), any());
		given(pf.createProducer()).willReturn(producer);
		GenericMessageListenerContainer container = mock(GenericMessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("two");
		given(container.getContainerProperties()).willReturn(properties);
		ReplyingKafkaTemplate template = new ReplyingKafkaTemplate(pf, container);
		template.start();
		Message<?> msg = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(KafkaHeaders.TOPIC, "foo")
				.build();
		long t1 = System.currentTimeMillis();
		CompletableFuture future = template.sendAndReceive(msg, Duration.ofMillis(10),
				new ParameterizedTypeReference<Foo>() {
				});
		try {
			future.get(10, TimeUnit.SECONDS);
		}
		catch (TimeoutException ex) {
			fail("get timed out");
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			fail("Interrupted");
		}
		catch (ExecutionException e) {
			assertThat(System.currentTimeMillis() - t1).isLessThan(3000L);
		}
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Autowired
		private EmbeddedKafkaBroker embeddedKafka;

		@Bean
		public DefaultKafkaProducerFactory<Integer, String> pf() {
			Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafka);
			producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L);
			return new DefaultKafkaProducerFactory<>(producerProps);
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, String> cf() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("serverSide", "false", this.embeddedKafka);
			return new DefaultKafkaConsumerFactory<>(consumerProps);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			return new KafkaTemplate<>(pf());
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf());
			factory.setReplyTemplate(template());
			factory.setReplyHeadersConfigurer((k, v) -> k.equals("baz"));
			factory.setMissingTopicsFatal(false);
			return factory;
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> customListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf());
			factory.setReplyTemplate(template());
			factory.setCorrelationHeaderName("customCorrelation");
			factory.setMissingTopicsFatal(false);
			return factory;
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<Integer, String> simpleMapperFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf());
			factory.setReplyTemplate(template());
			MessagingMessageConverter messageConverter = new MessagingMessageConverter();
			messageConverter.setHeaderMapper(new SimpleKafkaHeaderMapper());
			factory.setMessageConverter(messageConverter);
			factory.setReplyHeadersConfigurer(new ReplyHeadersConfigurer() {

				@Override
				public boolean shouldCopy(String headerName, Object headerValue) {
					return false;
				}

				@Override
				public Map<String, Object> additionalHeaders() {
					return Collections.singletonMap("qux", "fiz");
				}

			});
			return factory;
		}

		@KafkaListener(id = A_REQUEST, topics = A_REQUEST)
		@SendTo  // default REPLY_TOPIC header
		public String handleA(String in) throws InterruptedException {
			if (in.equals("slow")) {
				Thread.sleep(50);
			}
			if (in.equals("\"getAFoo\"")) {
				return ("{\"bar\":\"baz\"}");
			}
			if (in.equals("\"getFoos\"")) {
				return ("[{\"bar\":\"baz\"},{\"bar\":\"qux\"}]");
			}
			return in.toUpperCase();
		}

		@KafkaListener(topics = B_REQUEST, containerFactory = "simpleMapperFactory")
		@SendTo  // default REPLY_TOPIC header
		public String handleB(String in) {
			return in.substring(0, 1) + in.substring(1).toUpperCase();
		}

		@Bean
		public MultiMessageReturn mmr() {
			return new MultiMessageReturn();
		}

		@Bean
		public HandlerReturn handlerReturn() {
			return new HandlerReturn();
		}

		@KafkaListener(id = "def1", topics = { D_REQUEST, E_REQUEST, F_REQUEST },
				containerFactory = "customListenerContainerFactory")
		@SendTo  // default REPLY_TOPIC header
		public Message<String> dListener1(String in, @Header("customCorrelation") byte[] correlation) {
			return MessageBuilder.withPayload(in.toUpperCase())
					.setHeader("customCorrelation", correlation)
					.build();
		}

		@KafkaListener(id = "def2", topics = { D_REQUEST, E_REQUEST, F_REQUEST },
				containerFactory = "customListenerContainerFactory")
		@SendTo  // default REPLY_TOPIC header
		public Message<String> dListener2(String in) {
			return MessageBuilder.withPayload(in.substring(0, 1) + in.substring(1).toUpperCase())
					.build();
		}

		@KafkaListener(id = "def3", topics = { D_REQUEST, E_REQUEST, F_REQUEST },
				containerFactory = "customListenerContainerFactory")
		@SendTo  // default REPLY_TOPIC header
		public String dListener3(String in) {
			return in.substring(0, 1).toUpperCase() + in.substring(1);
		}

		@KafkaListener(id = G_REQUEST, topics = G_REQUEST)
		public void gListener(Message<String> in) {
			String replyTopic = new String(in.getHeaders().get("custom.reply.to",  byte[].class));
			int replyPart = ByteBuffer.wrap(in.getHeaders().get("custom.reply.partition", byte[].class)).getInt();
			ProducerRecord<Integer, String> record = new ProducerRecord<>(replyTopic, replyPart, null,
					in.getPayload() + "WithCustomHeaders");
			record.headers().add(new RecordHeader("custom.correlation.id",
					in.getHeaders().get("custom.correlation.id", byte[].class)));
			template().send(record);
		}

		@KafkaListener(id = H_REQUEST, topics = H_REQUEST)
		@SendTo  // default REPLY_TOPIC header
		public Message<?> messageReturn(String in) {
			return MessageBuilder.withPayload(in.toUpperCase())
					.setHeader(KafkaHeaders.KEY, 42)
					.build();
		}

		@KafkaListener(id = J_REQUEST, topics = J_REQUEST)
		@SendTo  // default REPLY_TOPIC header
		public String handleJ(String in) throws InterruptedException {
			return in.toUpperCase();
		}

		@KafkaListener(id = K_REQUEST, topics = { K_REQUEST })
		@SendTo
		public String handleK(ConsumerRecord<String, String> in) {
			return in.value().toUpperCase();
		}

		@KafkaListener(id = L_REQUEST, topics = L_REQUEST)
		@SendTo  // default REPLY_TOPIC header
		public Message<String> handleL(String in) throws InterruptedException {
			return MessageBuilder.withPayload(in.toUpperCase())
					.setHeader("serverSentAnError", "user error")
					.build();
		}

	}

	@KafkaListener(topics = C_REQUEST, groupId = C_REQUEST)
	@SendTo
	public static class MultiMessageReturn {

		@KafkaHandler
		public Message<?> listen1(String in, @Header(KafkaHeaders.REPLY_TOPIC) byte[] replyTo,
				@Header(KafkaHeaders.CORRELATION_ID) byte[] correlation) {

			return MessageBuilder.withPayload(in.toUpperCase())
					.setHeader(KafkaHeaders.TOPIC, replyTo)
					.setHeader(KafkaHeaders.KEY, 42)
					.setHeader(KafkaHeaders.CORRELATION_ID, correlation)
					.build();
		}

	}

	@KafkaListener(topics = I_REQUEST, groupId = I_REQUEST)
	public static class HandlerReturn {

		@KafkaHandler
		@SendTo
		public String listen1(String in) {
			return in.toUpperCase();
		}

	}

	public static class BadDeser implements Deserializer<Object> {

		@Override
		public Object deserialize(String topic, byte[] data) {
			return null;
		}

		@Override
		public Object deserialize(String topic, Headers headers, byte[] data) {
			throw new IllegalStateException("test reply deserialization failure");
		}

	}

	public static class Foo {

		private String bar;

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public String toString() {
			return "Foo [bar=" + this.bar + "]";
		}

	}

}
