/*
 * Copyright 2016-2022 the original author or authors.
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

package org.springframework.kafka.annotation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.context.annotation.Scope;
import org.springframework.context.event.EventListener;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.MethodParameter;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.web.JsonPath;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.event.ListenerContainerNoLongerIdleEvent;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.CommonLoggingErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.ProjectingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.MimeType;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dariusz Szablinski
 * @author Venil Noronha
 * @author Dimitri Penner
 * @author Nakul Mishra
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { "annotated1", "annotated2", "annotated3",
		"annotated4", "annotated5", "annotated6", "annotated7", "annotated8", "annotated8reply",
		"annotated9", "annotated10",
		"annotated11", "annotated12", "annotated13", "annotated14", "annotated15", "annotated16", "annotated17",
		"annotated18", "annotated19", "annotated20", "annotated21", "annotated21reply", "annotated22",
		"annotated22reply", "annotated23", "annotated23reply", "annotated24", "annotated24reply",
		"annotated25", "annotated25reply1", "annotated25reply2", "annotated26", "annotated27", "annotated28",
		"annotated29", "annotated30", "annotated30reply", "annotated31", "annotated32", "annotated33",
		"annotated34", "annotated35", "annotated36", "annotated37", "foo", "manualStart", "seekOnIdle",
		"annotated38", "annotated38reply", "annotated39", "annotated40", "annotated41", "annotated42",
		"annotated43", "annotated43reply" })
@TestPropertySource(properties = "spel.props=fetch.min.bytes=420000,max.poll.records=10")
public class EnableKafkaIntegrationTests {

	private static final String DEFAULT_TEST_GROUP_ID = "testAnnot";

	private static final Log logger = LogFactory.getLog(EnableKafkaIntegrationTests.class);

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private Config config;

	@Autowired
	public Listener listener;

	@Autowired
	public IfaceListenerImpl ifaceListener;

	@Autowired
	public MultiListenerBean multiListener;

	@Autowired
	public MultiJsonListenerBean multiJsonListener;

	@Autowired
	public MultiListenerNoDefault multiNoDefault;

	@Autowired
	public KafkaTemplate<Integer, String> template;

	@Autowired
	public KafkaTemplate<Integer, String> kafkaJsonTemplate;

	@Autowired
	public KafkaTemplate<byte[], String> bytesKeyTemplate;

	@Autowired
	public KafkaListenerEndpointRegistry registry;

	@Autowired
	private RecordPassAllFilter recordFilter;

	@Autowired
	private DefaultKafkaConsumerFactory<Integer, CharSequence> consumerFactory;

	@Autowired
	private AtomicReference<Consumer<?, ?>> consumerRef;

	@Autowired
	private List<?> quxGroup;

	@Autowired
	private FooConverter fooConverter;

	@Autowired
	private SeekToLastOnIdleListener seekOnIdleListener;

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private SmartMessageConverter fooContentConverter;

	@Autowired
	private RecordFilterStrategy<Integer, String> lambdaAll;

	@Test
	public void testAnonymous() {
		MessageListenerContainer container = this.registry
				.getListenerContainer("org.springframework.kafka.KafkaListenerEndpointContainer#0");
		List<?> containers = KafkaTestUtils.getPropertyValue(container, "containers", List.class);
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.consumerGroupId"))
				.isEqualTo(DEFAULT_TEST_GROUP_ID);
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.maxPollInterval"))
				.isEqualTo(300000L);
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.syncCommitTimeout"))
				.isEqualTo(Duration.ofSeconds(60));
		container.stop();
	}

	@SuppressWarnings("deprecation")
	@Test
	public void manyTests() throws Exception {
		this.recordFilter.called = false;
		template.send("annotated1", 0, "foo");
		template.send("annotated1", 0, "bar");
		assertThat(this.listener.latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.globalErrorThrowable).isNotNull();
		assertThat(this.listener.receivedGroupId).isEqualTo("foo");
		assertThat(this.listener.listenerInfo).isEqualTo("fee fi fo fum");
		assertThat(this.listener.latch1Batch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.batchOverrideStackTrace).contains("BatchMessagingMessageListenerAdapter");

		template.send("annotated2", 0, 123, "foo");
		assertThat(this.listener.latch2.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.key).isEqualTo(123);
		assertThat(this.listener.partition).isNotNull();
		assertThat(this.listener.topic).isEqualTo("annotated2");
		assertThat(this.listener.receivedGroupId).isEqualTo("bar");
		assertThat(this.listener.listenerInfo).isEqualTo("info for the bar listener");
		assertThat(this.meterRegistry.get("spring.kafka.template")
				.tag("name", "template")
				.tag("extraTag", "bar")
				.tag("result", "success")
				.timer()
				.count())
						.isGreaterThan(0L);

		assertThat(this.meterRegistry.get("spring.kafka.listener")
				.tag("name", "bar-0")
				.tag("extraTag", "foo")
				.tag("result", "success")
				.timer()
				.count())
						.isGreaterThan(0L);

		template.send("annotated3", 0, "foo");
		assertThat(this.listener.latch3.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.capturedRecord.value()).isEqualTo("foo");
		assertThat(this.config.listen3Exception).isNotNull();

		template.send("annotated4", 0, "foo");
		assertThat(this.listener.latch4.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.capturedRecord.value()).isEqualTo("foo");
		assertThat(this.listener.ack).isNotNull();
		assertThat(this.listener.eventLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.event.getListenerId().startsWith("qux-"));
		MessageListenerContainer manualContainer = this.registry.getListenerContainer("qux");
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener"))
				.isInstanceOf(FilteringMessageListenerAdapter.class);
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer, "containerProperties.messageListener.ackDiscarded",
				Boolean.class)).isTrue();
		assertThat(KafkaTestUtils.getPropertyValue(manualContainer,
				"containerProperties.messageListener.delegate"))
				.isInstanceOf(MessagingMessageListenerAdapter.class);
		assertThat(this.listener.listen4Consumer).isNotNull();
		assertThat(this.listener.listen4Consumer).isSameAs(KafkaTestUtils.getPropertyValue(KafkaTestUtils
						.getPropertyValue(this.registry.getListenerContainer("qux"), "containers", List.class).get(0),
				"listenerConsumer.consumer"));
		assertThat(
				KafkaTestUtils.getPropertyValue(this.listener.listen4Consumer, "fetcher.maxPollRecords", Integer.class))
				.isEqualTo(100);
		assertThat(this.quxGroup).hasSize(1);
		assertThat(this.quxGroup.get(0)).isSameAs(manualContainer);
		List<?> containers = KafkaTestUtils.getPropertyValue(manualContainer, "containers", List.class);
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.consumerGroupId"))
				.isEqualTo("qux");
		assertThat(KafkaTestUtils.getPropertyValue(containers.get(0), "listenerConsumer.consumer.clientId"))
				.isEqualTo("clientIdViaProps3-0");

		template.send("annotated4", 0, "foo");
		assertThat(this.listener.noLongerIdleEventLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.noLongerIdleEvent.getListenerId().startsWith("qux-"));

		template.send("annotated5", 0, 0, "foo");
		template.send("annotated5", 1, 0, "bar");
		template.send("annotated6", 0, 0, "baz");
		template.send("annotated6", 1, 0, "qux");
		template.flush();
		assertThat(this.listener.latch5.await(60, TimeUnit.SECONDS)).isTrue();
		MessageListenerContainer fizConcurrentContainer = registry.getListenerContainer("fiz");
		assertThat(fizConcurrentContainer).isNotNull();
		MessageListenerContainer fizContainer = (MessageListenerContainer) KafkaTestUtils
				.getPropertyValue(fizConcurrentContainer, "containers", List.class).get(0);
		TopicPartitionOffset offset = KafkaTestUtils.getPropertyValue(fizContainer, "topicPartitions",
				TopicPartitionOffset[].class)[2];
		assertThat(offset.isRelativeToCurrent()).isFalse();
		offset = KafkaTestUtils.getPropertyValue(fizContainer, "topicPartitions",
				TopicPartitionOffset[].class)[3];
		assertThat(offset.isRelativeToCurrent()).isTrue();
		assertThat(KafkaTestUtils.getPropertyValue(fizContainer,
						"listenerConsumer.consumer.groupId", Optional.class).get())
				.isEqualTo("fiz");
		assertThat(KafkaTestUtils.getPropertyValue(fizContainer, "listenerConsumer.consumer.clientId"))
				.isEqualTo("clientIdViaAnnotation-0");
		assertThat(KafkaTestUtils.getPropertyValue(fizContainer, "listenerConsumer.consumer.fetcher.maxPollRecords"))
				.isEqualTo(10);
		assertThat(KafkaTestUtils.getPropertyValue(fizContainer, "listenerConsumer.consumer.fetcher.minBytes"))
				.isEqualTo(420000);

		MessageListenerContainer rebalanceConcurrentContainer = registry.getListenerContainer("rebalanceListener");
		assertThat(rebalanceConcurrentContainer).isNotNull();
		assertThat(rebalanceConcurrentContainer.isAutoStartup()).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(rebalanceConcurrentContainer, "concurrency", Integer.class))
				.isEqualTo(3);
		rebalanceConcurrentContainer.start();

		template.send("annotated11", 0, "foo");
		assertThat(this.listener.latch7.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.consumerRef.get()).isNotNull();
		assertThat(this.listener.latch7String).isEqualTo("foo");

		assertThat(this.recordFilter.called).isTrue();

		template.send("annotated11", 0, null);
		assertThat(this.listener.latch7a.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.latch7String).isNull();

		MessageListenerContainer rebalanceContainer = (MessageListenerContainer) KafkaTestUtils
				.getPropertyValue(rebalanceConcurrentContainer, "containers", List.class).get(0);
		assertThat(KafkaTestUtils.getPropertyValue(rebalanceContainer, "listenerConsumer.consumer.groupId"))
				.isNotEqualTo("rebalanceListener");
		String clientId = KafkaTestUtils.getPropertyValue(rebalanceContainer, "listenerConsumer.consumer.clientId",
				String.class);
		assertThat(clientId).startsWith("rebal-");
		assertThat(clientId.indexOf('-')).isEqualTo(clientId.lastIndexOf('-'));
		FilteringMessageListenerAdapter<?, ?> adapter = (FilteringMessageListenerAdapter<?, ?>) registry
				.getListenerContainer("foo").getContainerProperties().getMessageListener();
		assertThat(adapter).extracting("recordFilterStrategy").isSameAs(this.lambdaAll);
		adapter = (FilteringMessageListenerAdapter<?, ?>) registry
				.getListenerContainer("bar").getContainerProperties().getMessageListener();
		assertThat(adapter).extracting("recordFilterStrategy").isSameAs(this.lambdaAll);
	}

	@Test
	public void testAutoStartup() {
		MessageListenerContainer listenerContainer = registry.getListenerContainer("manualStart");
		assertThat(listenerContainer).isNotNull();
		assertThat(listenerContainer.isRunning()).isFalse();
		assertThat(listenerContainer.getContainerProperties().getSyncCommitTimeout()).isNull();
		this.registry.start();
		assertThat(listenerContainer.isRunning()).isTrue();
		KafkaMessageListenerContainer<?, ?> kafkaMessageListenerContainer =
				((ConcurrentMessageListenerContainer<?, ?>) listenerContainer)
				.getContainers()
				.get(0);
		assertThat(kafkaMessageListenerContainer
				.getContainerProperties().getSyncCommitTimeout())
				.isEqualTo(Duration.ofSeconds(59));
		assertThat(listenerContainer.getContainerProperties().getSyncCommitTimeout())
				.isEqualTo(Duration.ofSeconds(59));
		listenerContainer.stop();
		assertThat(KafkaTestUtils.getPropertyValue(listenerContainer, "containerProperties.syncCommits", Boolean.class))
				.isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(listenerContainer, "containerProperties.commitCallback"))
				.isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(listenerContainer, "containerProperties.consumerRebalanceListener"))
				.isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(kafkaMessageListenerContainer, "listenerConsumer.maxPollInterval"))
				.isEqualTo(301000L);
	}

	@Test
	public void testInterface() throws Exception {
		template.send("annotated7", 0, "foo");
		template.flush();
		assertThat(this.ifaceListener.getLatch1().await(60, TimeUnit.SECONDS)).isTrue();
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testInterface");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated43reply");
		template.send("annotated43", 0, "foo");
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated43reply");
		assertThat(reply).extracting(rec -> rec.value()).isEqualTo("FOO");
		consumer.close();
	}

	@Test
	public void testMulti() throws Exception {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated8reply");
		this.template.send("annotated8", 0, 1, "foo");
		this.template.send("annotated8", 0, 1, null);
		this.template.flush();
		assertThat(this.multiListener.latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiListener.latch2.await(60, TimeUnit.SECONDS)).isTrue();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated8reply");
		assertThat(reply.value()).isEqualTo("OK");
		consumer.close();

		template.send("annotated8", 0, 1, "junk");
		assertThat(this.multiListener.errorLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiListener.meta).isNotNull();
	}

	@Test
	public void testMultiJson() throws Exception {
		this.kafkaJsonTemplate.setDefaultTopic("annotated33");
		this.kafkaJsonTemplate.send(new GenericMessage<>(new Foo("one")));
		this.kafkaJsonTemplate.send(new GenericMessage<>(new Baz("two")));
		this.kafkaJsonTemplate.send(new GenericMessage<>(new Qux("three")));
		this.kafkaJsonTemplate.send(new GenericMessage<>(new ValidatedClass(5)));
		assertThat(this.multiJsonListener.latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiJsonListener.latch2.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiJsonListener.latch3.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiJsonListener.latch4.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiJsonListener.foo.getBar()).isEqualTo("one");
		assertThat(this.multiJsonListener.baz.getBar()).isEqualTo("two");
		assertThat(this.multiJsonListener.bar.getBar()).isEqualTo("three");
		assertThat(this.multiJsonListener.bar).isInstanceOf(Qux.class);
		assertThat(this.multiJsonListener.validated).isNotNull();
		assertThat(this.multiJsonListener.validated.isValidated()).isTrue();
		assertThat(this.multiJsonListener.validated.valCount).isEqualTo(1);
	}

	@Test
	public void testMultiValidateNoDefaultHandler() throws Exception {
		this.kafkaJsonTemplate.setDefaultTopic("annotated40");
		this.kafkaJsonTemplate.send(new GenericMessage<>(new ValidatedClass(5)));
		assertThat(this.multiNoDefault.latch1.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.multiNoDefault.validated).isNotNull();
		assertThat(this.multiNoDefault.validated.isValidated()).isTrue();
		assertThat(this.multiNoDefault.validated.valCount).isEqualTo(1);
	}

	@Test
	public void testTx() throws Exception {
		template.send("annotated9", 0, "foo");
		template.flush();
		assertThat(this.ifaceListener.getLatch2().await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testJson() throws Exception {
		Foo foo = new Foo("bar");
		kafkaJsonTemplate.send(MessageBuilder.withPayload(foo)
				.setHeader(KafkaHeaders.TOPIC, "annotated10")
				.setHeader(KafkaHeaders.PARTITION, 0)
				.setHeader(KafkaHeaders.KEY, 2)
				.build());
		assertThat(this.listener.latch6.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.foo.getBar()).isEqualTo("bar");
		MessageListenerContainer buzConcurrentContainer = registry.getListenerContainer("buz");
		assertThat(buzConcurrentContainer).isNotNull();
		MessageListenerContainer buzContainer = (MessageListenerContainer) KafkaTestUtils
				.getPropertyValue(buzConcurrentContainer, "containers", List.class).get(0);
		assertThat(KafkaTestUtils.getPropertyValue(buzContainer,
						"listenerConsumer.consumer.groupId", Optional.class).get())
				.isEqualTo("buz.explicitGroupId");
		assertThat(KafkaTestUtils.getPropertyValue(buzContainer, "listenerConsumer.consumer.fetcher.maxPollRecords"))
				.isEqualTo(5);
		assertThat(KafkaTestUtils.getPropertyValue(buzContainer, "listenerConsumer.consumer.fetcher.minBytes"))
				.isEqualTo(123456);
	}

	@Test
	public void testJsonHeaders() throws Exception {
		ConcurrentMessageListenerContainer<?, ?> container =
				(ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer("jsonHeaders");
		Object messageListener = container.getContainerProperties().getMessageListener();
		DefaultJackson2JavaTypeMapper typeMapper = KafkaTestUtils.getPropertyValue(messageListener,
				"messageConverter.typeMapper", DefaultJackson2JavaTypeMapper.class);
		try {
			typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
			assertThat(container).isNotNull();
			Foo foo = new Foo("bar");
			this.kafkaJsonTemplate.send(MessageBuilder.withPayload(foo)
					.setHeader(KafkaHeaders.TOPIC, "annotated31")
					.setHeader(KafkaHeaders.PARTITION, 0)
					.setHeader(KafkaHeaders.KEY, 2)
					.build());
			assertThat(this.listener.latch19.await(60, TimeUnit.SECONDS)).isTrue();
			assertThat(this.listener.foo.getBar()).isEqualTo("bar");
		}
		finally {
			typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.INFERRED);
		}
	}

	@Test
	public void testNulls() throws Exception {
		template.send("annotated12", null, null);
		assertThat(this.listener.latch8.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testEmpty() throws Exception {
		template.send("annotated13", null, "");
		assertThat(this.listener.latch9.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testBatch() throws Exception {
		this.recordFilter.called = false;
		template.send("annotated14", null, "foo");
		template.send("annotated14", null, "bar");
		assertThat(this.listener.latch10.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(String.class);
		assertThat(this.recordFilter.batchCalled).isTrue();
		assertThat(this.config.listen10Exception).isNotNull();
		assertThat(this.listener.receivedGroupId).isEqualTo("list1");

		assertThat(this.config.spyLatch.await(30, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testBatchWitHeaders() throws Exception {
		template.send("annotated15", 0, 1, "foo");
		template.send("annotated15", 0, 1, "bar");
		assertThat(this.listener.latch11.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(String.class);
		list = this.listener.keys;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Integer.class);
		list = this.listener.partitions;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Integer.class);
		list = this.listener.topics;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(String.class);
		list = this.listener.offsets;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Long.class);
		assertThat(this.listener.listenerInfo).isEqualTo("info for batch");
	}

	@Test
	public void testBatchRecords() throws Exception {
		template.send("annotated16", null, "foo");
		template.send("annotated16", null, "bar");
		assertThat(this.listener.latch12.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(ConsumerRecord.class);
		assertThat(this.listener.listen12Consumer).isNotNull();
		assertThat(this.listener.listen12Consumer).isSameAs(KafkaTestUtils.getPropertyValue(KafkaTestUtils
						.getPropertyValue(this.registry.getListenerContainer("list3"), "containers", List.class).get(0),
				"listenerConsumer.consumer"));
		assertThat(this.config.listen12Latch.await(10, TimeUnit.SECONDS)).isNotNull();
		assertThat(this.config.listen12Exception).isNotNull();
		assertThat(this.config.listen12Message.getPayload()).isInstanceOf(List.class);
		List<?> errorPayload = (List<?>) this.config.listen12Message.getPayload();
		assertThat(errorPayload.size()).isGreaterThanOrEqualTo(1);
		assertThat(this.config.batchIntercepted).isTrue();
	}

	@Test
	public void testBatchRecordsAck() throws Exception {
		template.send("annotated17", null, "foo");
		template.send("annotated17", null, "bar");
		assertThat(this.listener.latch13.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(ConsumerRecord.class);
		assertThat(this.listener.ack).isNotNull();
		assertThat(this.listener.listen13Consumer).isNotNull();
		assertThat(this.listener.listen13Consumer).isSameAs(KafkaTestUtils.getPropertyValue(KafkaTestUtils
						.getPropertyValue(this.registry.getListenerContainer("list4"), "containers", List.class).get(0),
				"listenerConsumer.consumer"));
	}

	@Test
	public void testBatchMessages() throws Exception {
		template.send("annotated18", null, "foo");
		template.send("annotated18", null, "bar");
		assertThat(this.listener.latch14.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Message.class);
		Message<?> m = (Message<?>) list.get(0);
		assertThat(m.getPayload()).isInstanceOf(String.class);
	}

	@Test
	public void testBatchMessagesAck() throws Exception {
		template.send("annotated19", null, "foo");
		template.send("annotated19", null, "bar");
		assertThat(this.listener.latch15.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.payload).isInstanceOf(List.class);
		List<?> list = (List<?>) this.listener.payload;
		assertThat(list.size()).isGreaterThan(0);
		assertThat(list.get(0)).isInstanceOf(Message.class);
		Message<?> m = (Message<?>) list.get(0);
		assertThat(m.getPayload()).isInstanceOf(String.class);
		assertThat(this.listener.ack).isNotNull();
	}

	@Test
	public void testListenerErrorHandler() throws Exception {
		template.send("annotated20", 0, "foo");
		template.flush();
		assertThat(this.listener.latch16.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testValidation() throws Exception {
		template.send("annotated35", 0, "{\"bar\":42}");
		assertThat(this.listener.validationLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.validationException).isInstanceOf(MethodArgumentNotValidException.class);
	}

	@Test
	public void testReplyingListener() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated21reply");
		template.send("annotated21", 0, "nnotated21reply"); // drop the leading 'a'.
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated21reply");
		assertThat(reply.value()).isEqualTo("NNOTATED21REPLY");
		consumer.close();
	}

	@Test
	public void testReplyingBatchListener() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testBatchReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated22reply");
		template.send("annotated22", 0, 0, "foo");
		template.send("annotated22", 0, 0, "bar");
		template.flush();
		ConsumerRecords<Integer, String> replies = KafkaTestUtils.getRecords(consumer);
		assertThat(replies.count()).isGreaterThanOrEqualTo(1);
		Iterator<ConsumerRecord<Integer, String>> iterator = replies.iterator();
		assertThat(iterator.next().value()).isEqualTo("FOO");
		if (iterator.hasNext()) {
			assertThat(iterator.next().value()).isEqualTo("BAR");
		}
		else {
			replies = KafkaTestUtils.getRecords(consumer);
			assertThat(replies.count()).isGreaterThanOrEqualTo(1);
			iterator = replies.iterator();
			assertThat(iterator.next().value()).isEqualTo("BAR");
		}
		consumer.close();
	}

	@Test
	public void testReplyingListenerWithErrorHandler() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testErrorHandlerReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated23reply");
		template.send("annotated23", 0, "FoO");
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated23reply");
		assertThat(reply.value()).isEqualTo("foo");
		consumer.close();
	}

	@Test
	public void testVoidListenerWithReplyingErrorHandler() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testVoidWithErrorHandlerReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated30reply");
		template.send("annotated30", 0, "FoO");
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated30reply");
		assertThat(reply.value()).isEqualTo("baz");
		consumer.close();
	}

	@Test
	public void testReplyingBatchListenerWithErrorHandler() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testErrorHandlerBatchReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated24reply");
		template.send("annotated24", 0, 0, "FoO");
		template.send("annotated24", 0, 0, "BaR");
		template.flush();
		ConsumerRecords<Integer, String> replies = KafkaTestUtils.getRecords(consumer);
		assertThat(replies.count()).isGreaterThanOrEqualTo(1);
		Iterator<ConsumerRecord<Integer, String>> iterator = replies.iterator();
		assertThat(iterator.next().value()).isEqualTo("foo");
		if (iterator.hasNext()) {
			assertThat(iterator.next().value()).isEqualTo("bar");
		}
		else {
			replies = KafkaTestUtils.getRecords(consumer);
			assertThat(replies.count()).isGreaterThanOrEqualTo(1);
			iterator = replies.iterator();
			assertThat(iterator.next().value()).isEqualTo("bar");
		}
		consumer.close();
	}

	@Test
	public void testMultiReplyTo() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testMultiReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromEmbeddedTopics(consumer, "annotated25reply1", "annotated25reply2");
		template.send("annotated25", 0, 1, "foo");
		template.flush();
		ConsumerRecord<Integer, String> reply = KafkaTestUtils.getSingleRecord(consumer, "annotated25reply1");
		assertThat(reply.value()).isEqualTo("FOO");
		template.send("annotated25", 0, 1, null);
		reply = KafkaTestUtils.getSingleRecord(consumer, "annotated25reply2");
		assertThat(reply.value()).isEqualTo("BAR");
		consumer.close();
	}

	@Test
	public void testBatchAck() throws Exception {
		template.send("annotated26", 0, 1, "foo1");
		template.send("annotated27", 0, 1, "foo2");
		template.send("annotated27", 0, 1, "foo3");
		template.send("annotated27", 0, 1, "foo4");
		template.flush();
		assertThat(this.listener.latch17.await(60, TimeUnit.SECONDS)).isTrue();
		template.send("annotated26", 0, 1, "foo5");
		template.send("annotated27", 0, 1, "foo6");
		template.flush();
		assertThat(this.listener.latch18.await(60, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void testBadAckConfig() throws Exception {
		template.send("annotated28", 0, 1, "foo1");
		assertThat(this.config.badAckLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.badAckException).isInstanceOf(IllegalStateException.class);
		assertThat(this.config.badAckException.getMessage())
				.isEqualTo("No Acknowledgment available as an argument, "
						+ "the listener container must have a MANUAL AckMode to populate the Acknowledgment.");
	}

	@Test
	public void testConverterBean() throws Exception {
		@SuppressWarnings("unchecked")
		Converter<String, Foo> converterDelegate = mock(Converter.class);
		fooConverter.setDelegate(converterDelegate);

		Foo foo = new Foo("foo");
		willReturn(foo).given(converterDelegate).convert("{'bar':'foo'}");
		template.send("annotated32", 0, 1, "{'bar':'foo'}");
		assertThat(this.listener.latch20.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.listen16foo).isEqualTo(foo);

		willThrow(new RuntimeException()).given(converterDelegate).convert("foobar");
		template.send("annotated32", 0, 1, "foobar");
		assertThat(this.config.listen16ErrorLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.listen16Exception).isNotNull();
		assertThat(this.config.listen16Exception).isInstanceOf(ListenerExecutionFailedException.class);
		assertThat(((ListenerExecutionFailedException) this.config.listen16Exception).getGroupId())
				.isEqualTo("converter.explicitGroupId");
		assertThat(this.config.listen16Message).isEqualTo("foobar");
	}

	@Test
	public void testAddingTopics() {
		int count = this.embeddedKafka.getTopics().size();
		Map<String, Exception> results = this.embeddedKafka.addTopicsWithResults("testAddingTopics");
		assertThat(results).hasSize(1);
		assertThat(results.keySet().iterator().next()).isEqualTo("testAddingTopics");
		assertThat(results.get("testAddingTopics")).isNull();
		assertThat(this.embeddedKafka.getTopics().size()).isEqualTo(count + 1);
		results = this.embeddedKafka.addTopicsWithResults("testAddingTopics");
		assertThat(results).hasSize(1);
		assertThat(results.keySet().iterator().next()).isEqualTo("testAddingTopics");
		assertThat(results.get("testAddingTopics"))
				.isInstanceOf(ExecutionException.class)
				.hasCauseInstanceOf(TopicExistsException.class);
		assertThat(this.embeddedKafka.getTopics().size()).isEqualTo(count + 1);
		results = this.embeddedKafka.addTopicsWithResults(new NewTopic("morePartitions", 10, (short) 1));
		assertThat(results).hasSize(1);
		assertThat(results.keySet().iterator().next()).isEqualTo("morePartitions");
		assertThat(results.get("morePartitions")).isNull();
		assertThat(this.embeddedKafka.getTopics().size()).isEqualTo(count + 2);
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.embeddedKafka.addTopics(new NewTopic("morePartitions", 10, (short) 1)))
				.withMessageContaining("exists");
		assertThatIllegalArgumentException()
				.isThrownBy(() -> this.embeddedKafka.addTopics(new NewTopic("morePartitions2", 10, (short) 2)))
				.withMessageContaining("replication");
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testMultiReplying");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, String> consumer = cf.createConsumer();
		assertThat(consumer.partitionsFor("morePartitions")).hasSize(10);
		consumer.close();
	}

	@Test
	public void testReceivePollResults() throws Exception {
		this.template.send("annotated34", "allRecords");
		assertThat(this.listener.latch21.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.consumerRecords).isNotNull();
		assertThat(this.listener.consumerRecords.count()).isEqualTo(1);
		assertThat(this.listener.consumerRecords.iterator().next().value()).isEqualTo("allRecords");
	}

	@Test
	public void testKeyConversion() throws Exception {
		this.bytesKeyTemplate.send("annotated36", "foo".getBytes(), "bar");
		assertThat(this.listener.keyLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.convertedKey).isEqualTo("foo");
		assertThat(this.config.intercepted).isTrue();
		try {
			assertThat(this.meterRegistry.get("kafka.consumer.coordinator.join.total")
					.tag("consumerTag", "bytesString")
					.tag("spring.id", "bytesStringConsumerFactory.tag-0")
					.functionCounter()
					.count())
						.isGreaterThan(0);

			assertThat(this.meterRegistry.get("kafka.producer.incoming.byte.total")
					.tag("producerTag", "bytesString")
					.tag("spring.id", "bytesStringProducerFactory.bsPF-1")
					.functionCounter()
					.count())
						.isGreaterThan(0);
		}
		catch (Exception e) {
			logger.error(this.meterRegistry.getMeters()
					.stream()
					.map(meter -> "\n" + meter.getId())
					.collect(Collectors.toList()), e);
			throw e;
		}
		MessageListenerContainer container = this.registry.getListenerContainer("bytesKey");
		assertThat(KafkaTestUtils.getPropertyValue(container, "commonErrorHandler"))
				.isInstanceOf(CommonLoggingErrorHandler.class);
	}

	@Test
	public void testProjection() throws InterruptedException {
		template.send("annotated37", 0, "{ \"username\" : \"SomeUsername\", \"user\" : { \"name\" : \"SomeName\"}}");
		assertThat(this.listener.projectionLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.name).isEqualTo("SomeName");
		assertThat(this.listener.username).isEqualTo("SomeUsername");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSeekToLastOnIdle() throws InterruptedException {
		this.registry.getListenerContainer("seekOnIdle").start();
		this.seekOnIdleListener.waitForBalancedAssignment();
		this.template.send("seekOnIdle", 0, 0, "foo");
		this.template.send("seekOnIdle", 1, 1, "bar");
		assertThat(this.seekOnIdleListener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.seekOnIdleListener.latch2.getCount()).isEqualTo(2L);
		this.seekOnIdleListener.rewindAllOneRecord();
		assertThat(this.seekOnIdleListener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.seekOnIdleListener.latch3.getCount()).isEqualTo(1L);
		this.seekOnIdleListener.rewindOnePartitionOneRecord("seekOnIdle", 1);
		assertThat(this.seekOnIdleListener.latch3.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.getListenerContainer("seekOnIdle").stop();
		assertThat(this.seekOnIdleListener.latch4.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(KafkaTestUtils.getPropertyValue(this.seekOnIdleListener, "callbacks", Map.class)).hasSize(0);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReplyingBatchListenerReturnCollection() {
		Map<String, Object> consumerProps = new HashMap<>(this.consumerFactory.getConfigurationProperties());
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "testReplyingBatchListenerReturnCollection");
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		ConsumerFactory<Integer, Object> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		Consumer<Integer, Object> consumer = cf.createConsumer();
		this.embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "annotated38reply");
		template.send("annotated38", 0, 0, "FoO");
		template.send("annotated38", 0, 0, "BaR");
		template.flush();
		ConsumerRecords replies = KafkaTestUtils.getRecords(consumer);
		assertThat(replies.count()).isGreaterThanOrEqualTo(1);
		Iterator<ConsumerRecord<?, ?>> iterator = replies.iterator();
		Object value = iterator.next().value();
		assertThat(value).isInstanceOf(List.class);
		List list = (List) value;
		assertThat(list).hasSizeGreaterThanOrEqualTo(1);
		assertThat(list.get(0)).isEqualTo("FOO");
		if (list.size() > 1) {
			assertThat(list.get(1)).isEqualTo("BAR");
		}
		else {
			replies = KafkaTestUtils.getRecords(consumer);
			assertThat(replies.count()).isGreaterThanOrEqualTo(1);
			iterator = replies.iterator();
			value = iterator.next().value();
			list = (List) value;
			assertThat(list).hasSize(1);
			assertThat(list.get(0)).isEqualTo("BAR");
		}
		consumer.close();
	}

	@Test
	public void testCustomMethodArgumentResovlerListener() throws InterruptedException {
		template.send("annotated39", "foo");
		assertThat(this.listener.customMethodArgumentResolverLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.customMethodArgument.body).isEqualTo("foo");
		assertThat(this.listener.customMethodArgument.topic).isEqualTo("annotated39");
	}

	@Test
	public void testContentConversion() throws InterruptedException {
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(this.template.getProducerFactory());
		template.setMessagingConverter(this.fooContentConverter);
		template.send(MessageBuilder.withPayload(new Foo("bar"))
				.setHeader(KafkaHeaders.TOPIC, "annotated41")
				.setHeader(MessageHeaders.CONTENT_TYPE, "application/foo")
				.build());
		assertThat(this.listener.contentLatch.await(30, TimeUnit.SECONDS)).isTrue();
		assertThat(this.listener.contentFoo).isEqualTo(new Foo("bar"));
	}

	@Test
	void proto(@Autowired ApplicationContext context) {
		this.registry.setAlwaysStartAfterRefresh(false);
		context.getBean(ProtoListener.class);
		assertThat(this.registry.getListenerContainer("proto").isRunning()).isFalse();
		this.registry.setAlwaysStartAfterRefresh(true);
	}

	@Test
	void classLevelTwoInstancesSameClass() {
		assertThat(this.registry.getListenerContainer("multiTwoOne")).isNotNull();
		assertThat(this.registry.getListenerContainer("multiTwoTwo")).isNotNull();
	}

	@Configuration
	@EnableKafka
	@EnableTransactionManagement(proxyTargetClass = true)
	public static class Config implements KafkaListenerConfigurer {

		final CountDownLatch spyLatch = new CountDownLatch(2);

		volatile Throwable globalErrorThrowable;

		volatile boolean intercepted;

		volatile boolean batchIntercepted;

		@Autowired
		private EmbeddedKafkaBroker embeddedKafka;

		@SuppressWarnings("unchecked")
		@Bean
		public MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		public static PropertySourcesPlaceholderConfigurer ppc() {
			return new PropertySourcesPlaceholderConfigurer();
		}

		@Bean
		public PlatformTransactionManager transactionManager() {
			return mock(PlatformTransactionManager.class);
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setRecordFilterStrategy(recordFilter());
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			factory.setCommonErrorHandler(new DefaultErrorHandler() {

				@Override
				public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
						Consumer<?, ?> consumer, MessageListenerContainer container) {

					Config.this.globalErrorThrowable = thrownException;
					super.handleRemaining(thrownException, records, consumer, container);
				}

			});
			factory.getContainerProperties().setMicrometerTags(Collections.singletonMap("extraTag", "foo"));
			// ensure annotation wins, even with explicitly set here
			factory.setBatchListener(false);
			return factory;
		}

		@Bean
		@SuppressWarnings("deprecation")
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				factoryWithBadConverter() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setRecordFilterStrategy(recordFilter());
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			factory.setCommonErrorHandler(new CommonErrorHandler() {

				@Override
				public void handleRecord(Exception thrownException, ConsumerRecord<?, ?> record,
						Consumer<?, ?> consumer, MessageListenerContainer container) {

					globalErrorThrowable = thrownException;
					consumer.seek(new org.apache.kafka.common.TopicPartition(record.topic(), record.partition()),
							record.offset());
				}
			});
			factory.getContainerProperties().setMicrometerTags(Collections.singletonMap("extraTag", "foo"));
			factory.setMessageConverter(new RecordMessageConverter() {

				@Override
				public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment,
						Consumer<?, ?> consumer, Type payloadType) {

					throw new UnsupportedOperationException();
				}

				@Override
				public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
					throw new UnsupportedOperationException();				}

			});
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				withNoReplyTemplateContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public RecordPassAllFilter recordFilter() {
			return new RecordPassAllFilter();
		}

		@Bean
		public RecordPassAllFilter manualFilter() {
			return new RecordPassAllFilter();
		}

		@Bean
		public KafkaListenerContainerFactory<?> kafkaJsonListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			JsonMessageConverter converter = new JsonMessageConverter();
			DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
			typeMapper.addTrustedPackages("*");
			converter.setTypeMapper(typeMapper);
			factory.setMessageConverter(converter);
			return factory;
		}

		/*
		 * Uses Type_Id header
		 */
		@Bean
		public KafkaListenerContainerFactory<?> kafkaJsonListenerContainerFactory2() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			JsonMessageConverter converter = new JsonMessageConverter();
			DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
			typeMapper.addTrustedPackages("*");
			typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
			converter.setTypeMapper(typeMapper);
			factory.setMessageConverter(converter);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> projectionListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			JsonMessageConverter converter = new JsonMessageConverter();
			DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
			typeMapper.addTrustedPackages("*");
			converter.setTypeMapper(typeMapper);
			factory.setMessageConverter(new ProjectingMessageConverter(converter));
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> bytesStringListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory<byte[], String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(bytesStringConsumerFactory());
			factory.setRecordInterceptor((record, consumer) -> {
				this.intercepted = true;
				return record;
			});
			factory.setCommonErrorHandler(new CommonLoggingErrorHandler());
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.setRecordFilterStrategy(recordFilter());
			// always send to the same partition so the replies are in order for the test
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			factory.setBatchInterceptor((records, consumer) -> {
				this.batchIntercepted = true;
				return records;
			});
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchJsonReplyFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.setRecordFilterStrategy(recordFilter());
			// always send to the same partition so the replies are in order for the test
			factory.setReplyTemplate(partitionZeroReplyJsonTemplate());
			return factory;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Bean
		public KafkaListenerContainerFactory<?> batchSpyFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			ConsumerFactory spiedCf = mock(ConsumerFactory.class);
			willAnswer(i -> {
				Consumer<Integer, CharSequence> spy =
						spy(consumerFactory().createConsumer(i.getArgument(0), i.getArgument(1),
								i.getArgument(2), i.getArgument(3)));
				willAnswer(invocation -> {

					try {
						return invocation.callRealMethod();
					}
					finally {
						spyLatch.countDown();
					}

				}).given(spy).commitSync(anyMap(), any());
				return spy;
			}).given(spiedCf).createConsumer(anyString(), anyString(), anyString(), any());
			factory.setConsumerFactory(spiedCf);
			factory.setBatchListener(true);
			factory.setRecordFilterStrategy(recordFilter());
			// always send to the same partition so the replies are in order for the test
			factory.setReplyTemplate(partitionZeroReplyTemplate());
			factory.setMissingTopicsFatal(false);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchManualFactory() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps1"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.MANUAL_IMMEDIATE);
			factory.setBatchListener(true);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<?> batchManualFactory2() {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps2"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.MANUAL_IMMEDIATE);
			factory.setBatchListener(true);
			return factory;
		}

		@Bean
		@SuppressWarnings("deprecation")
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaManualAckListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps3"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.MANUAL_IMMEDIATE);
			props.setIdleEventInterval(100L);
			props.setPollTimeout(50L);
			factory.setRecordFilterStrategy(manualFilter());
			factory.setAckDiscarded(true);
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaAutoStartFalseListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			ContainerProperties props = factory.getContainerProperties();
			factory.setConsumerFactory(consumerFactory());
			factory.setAutoStartup(false);
			props.setSyncCommits(false);
			props.setCommitCallback(mock(OffsetCommitCallback.class));
			props.setConsumerRebalanceListener(mock(ConsumerRebalanceListener.class));
			return factory;
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				kafkaRebalanceListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			ContainerProperties props = factory.getContainerProperties();
			factory.setAutoStartup(true);
			factory.setConsumerFactory(configuredConsumerFactory("rebal"));
			props.setConsumerRebalanceListener(consumerRebalanceListener(consumerRef()));
			return factory;
		}

		@Bean
		@SuppressWarnings("deprecation")
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
				recordAckListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(configuredConsumerFactory("clientIdViaProps4"));
			ContainerProperties props = factory.getContainerProperties();
			props.setAckMode(AckMode.RECORD);
			factory.setCommonErrorHandler(listen16ErrorHandler());
			return factory;
		}

		@Bean
		public DefaultKafkaConsumerFactory<Integer, CharSequence> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public DefaultKafkaConsumerFactory<byte[], String> bytesStringConsumerFactory() {
			Map<String, Object> configs = consumerConfigs();
			configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			DefaultKafkaConsumerFactory<byte[], String> cf = new DefaultKafkaConsumerFactory<>(configs);
			cf.addListener(new MicrometerConsumerListener<byte[], String>(meterRegistry(),
					Collections.singletonList(new ImmutableTag("consumerTag", "bytesString"))));
			return cf;
		}

		private ConsumerFactory<Integer, String> configuredConsumerFactory(String clientAndGroupId) {
			Map<String, Object> configs = consumerConfigs();
			configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
			configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientAndGroupId);
			configs.put(ConsumerConfig.GROUP_ID_CONFIG, clientAndGroupId);
			return new DefaultKafkaConsumerFactory<>(configs);
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps =
					KafkaTestUtils.consumerProps(DEFAULT_TEST_GROUP_ID, "false", this.embeddedKafka);
			return consumerProps;
		}

		@Bean
		public Listener listener() {
			return new Listener();
		}

		@Bean
		public SeekToLastOnIdleListener seekOnIdle() {
			return new SeekToLastOnIdleListener();
		}

		@Bean
		public IfaceListener<String> ifaceListener() {
			return new IfaceListenerImpl();
		}

		@Bean
		@Order(Ordered.HIGHEST_PRECEDENCE)
		@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
		public ProxyListenerPostProcessor proxyListenerPostProcessor() {
			return new ProxyListenerPostProcessor();
		}

		@Bean
		public MultiListenerBean multiListener() {
			return new MultiListenerBean();
		}

		@Bean
		public MultiJsonListenerBean multiJsonListener() {
			return new MultiJsonListenerBean();
		}

		@Bean
		public MultiListenerNoDefault multiNoDefault() {
			return new MultiListenerNoDefault();
		}

		@Bean
		public MultiListenerSendToImpl multiListenerSendTo() {
			return new MultiListenerSendToImpl();
		}

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public ProducerFactory<Integer, Object> jsonProducerFactory() {
			Map<String, Object> producerConfigs = producerConfigs();
			producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
			return new DefaultKafkaProducerFactory<>(producerConfigs);
		}

		@Bean
		public ProducerFactory<Integer, String> txProducerFactory() {
			DefaultKafkaProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerConfigs());
			pf.setTransactionIdPrefix("tx-");
			return pf;
		}

		@Bean
		public ProducerFactory<byte[], String> bytesStringProducerFactory() {
			Map<String, Object> configs = producerConfigs();
			configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
			configs.put(ProducerConfig.CLIENT_ID_CONFIG, "bsPF");
			DefaultKafkaProducerFactory<byte[], String> pf = new DefaultKafkaProducerFactory<>(configs);
			pf.addListener(new MicrometerProducerListener<byte[], String>(meterRegistry(),
					Collections.singletonList(new ImmutableTag("producerTag", "bytesString"))));
			return pf;
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(this.embeddedKafka);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			kafkaTemplate.setMicrometerTags(Collections.singletonMap("extraTag", "bar"));
			return kafkaTemplate;
		}

		@Bean
		public KafkaTemplate<byte[], String> bytesKeyTemplate() {
			return new KafkaTemplate<>(bytesStringProducerFactory());
		}

		@Bean
		public KafkaTemplate<Integer, String> partitionZeroReplyTemplate() {
			// reply always uses the no-partition, no-key method; subclasses can be used
			return new KafkaTemplate<Integer, String>(producerFactory(), true) {

				@Override
				public CompletableFuture<SendResult<Integer, String>> send(String topic, String data) {
					return super.send(topic, 0, null, data);
				}

			};
		}

		@Bean
		public KafkaTemplate<Integer, Object> partitionZeroReplyJsonTemplate() {
			// reply always uses the no-partition, no-key method; subclasses can be used
			return new KafkaTemplate<Integer, Object>(jsonProducerFactory(), true) {

				@Override
				public CompletableFuture<SendResult<Integer, Object>> send(String topic, Object data) {
					return super.send(topic, 0, null, data);
				}

			};
		}

		@Bean
		public KafkaTemplate<Integer, String> kafkaJsonTemplate() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
			kafkaTemplate.setMessageConverter(new StringJsonMessageConverter());
			return kafkaTemplate;
		}

		@Bean
		public AtomicReference<Consumer<?, ?>> consumerRef() {
			return new AtomicReference<>();
		}

		private ConsumerAwareRebalanceListener consumerRebalanceListener(
				final AtomicReference<Consumer<?, ?>> consumerRef) {
			return new ConsumerAwareRebalanceListener() {

				@Override
				public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
						Collection<org.apache.kafka.common.TopicPartition> partitions) {
					consumerRef.set(consumer);
				}

				@Override
				public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer,
						Collection<org.apache.kafka.common.TopicPartition> partitions) {
					consumerRef.set(consumer);
				}

				@Override
				public void onPartitionsAssigned(Consumer<?, ?> consumer,
						Collection<org.apache.kafka.common.TopicPartition> partitions) {
					consumerRef.set(consumer);
				}

			};
		}

		@Bean
		public KafkaListenerErrorHandler consumeException(Listener listener) {
			return (m, e) -> {
				listener.latch16.countDown();
				return null;
			};
		}

		@Bean
		public KafkaListenerErrorHandler validationErrorHandler(Listener listener) {
			return (m, e) -> {
				listener.validationException = (Exception) e.getCause();
				listener.validationLatch.countDown();
				return null;
			};
		}

		@Bean
		public KafkaListenerErrorHandler replyErrorHandler() {
			return (m, e) -> ((String) m.getPayload()).toLowerCase();
		}

		@SuppressWarnings("unchecked")
		@Bean
		public KafkaListenerErrorHandler replyBatchErrorHandler() {
			return (m, e) -> ((Collection<String>) m.getPayload())
					.stream()
					.map(String::toLowerCase)
					.collect(Collectors.toList());
		}

		private ListenerExecutionFailedException listen3Exception;

		private ListenerExecutionFailedException listen10Exception;

		private ListenerExecutionFailedException listen12Exception;

		@Bean
		public ConsumerAwareListenerErrorHandler listen3ErrorHandler() {
			return (m, e, c) -> {
				this.listen3Exception = e;
				MessageHeaders headers = m.getHeaders();
				c.seek(new org.apache.kafka.common.TopicPartition(
								headers.get(KafkaHeaders.RECEIVED_TOPIC, String.class),
								headers.get(KafkaHeaders.RECEIVED_PARTITION, Integer.class)),
						headers.get(KafkaHeaders.OFFSET, Long.class));
				return null;
			};
		}

		@Bean
		public ConsumerAwareListenerErrorHandler listen10ErrorHandler() {
			return (m, e, c) -> {
				this.listen10Exception = e;
				resetAllOffsets(m, c);
				return null;
			};
		}

		private Message<?> listen12Message;

		private final CountDownLatch listen12Latch = new CountDownLatch(1);

		@Bean
		public ConsumerAwareListenerErrorHandler listen12ErrorHandler() {
			return (m, e, c) -> {
				this.listen12Exception = e;
				this.listen12Message = m;
				resetAllOffsets(m, c);
				this.listen12Latch.countDown();
				return null;
			};
		}

		protected void resetAllOffsets(Message<?> message, Consumer<?, ?> consumer) {
			MessageHeaders headers = message.getHeaders();
			@SuppressWarnings("unchecked")
			List<String> topics = headers.get(KafkaHeaders.RECEIVED_TOPIC, List.class);
			if (topics == null) {
				return;
			}
			@SuppressWarnings("unchecked")
			List<Integer> partitions = headers.get(KafkaHeaders.RECEIVED_PARTITION, List.class);
			@SuppressWarnings("unchecked")
			List<Long> offsets = headers.get(KafkaHeaders.OFFSET, List.class);
			Map<org.apache.kafka.common.TopicPartition, Long> offsetsToReset = new HashMap<>();
			for (int i = 0; i < topics.size(); i++) {
				int index = i;
				offsetsToReset.compute(new org.apache.kafka.common.TopicPartition(topics.get(i), partitions.get(i)),
						(k, v) -> v == null ? offsets.get(index) : Math.min(v, offsets.get(index)));
			}
			offsetsToReset.forEach(consumer::seek);
		}

		private final CountDownLatch badAckLatch = new CountDownLatch(1);

		private Throwable badAckException;

		@Bean
		public KafkaListenerErrorHandler badAckConfigErrorHandler() {
			return (m, e) -> {
				this.badAckException = e.getCause();
				this.badAckLatch.countDown();
				return "baz";
			};
		}

		@Bean
		public KafkaListenerErrorHandler voidSendToErrorHandler() {
			return (m, e) -> {
				return "baz";
			};
		}

		private Throwable listen16Exception;

		private Object listen16Message;

		private final CountDownLatch listen16ErrorLatch = new CountDownLatch(1);

		@Bean
		public CommonErrorHandler listen16ErrorHandler() {
			return new CommonErrorHandler() {

				@Override
				public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record,
						Consumer<?, ?> consumer, MessageListenerContainer container) {

					listen16Exception = thrownException;
					listen16Message = record.value();
					listen16ErrorLatch.countDown();
					return true;
				}

			};
		}

		@Bean
		public FooConverter fooConverter() {
			return new FooConverter();
		}

		@Override
		public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
			registrar.setValidator(new Validator() {

				@Override
				public void validate(Object target, Errors errors) {
					if (target instanceof ValidatedClass && ((ValidatedClass) target).getBar() > 10) {
						errors.reject("bar too large");
					}
					else {
						((ValidatedClass) target).setValidated(true);
					}
				}

				@Override
				public boolean supports(Class<?> clazz) {
					return ValidatedClass.class.isAssignableFrom(clazz);
				}

			});
			registrar.setCustomMethodArgumentResolvers(
					new HandlerMethodArgumentResolver() {

						@Override
						public boolean supportsParameter(MethodParameter parameter) {
							return CustomMethodArgument.class.isAssignableFrom(parameter.getParameterType());
						}

						@Override
						public Object resolveArgument(MethodParameter parameter, Message<?> message) {
							return new CustomMethodArgument(
									(String) message.getPayload(),
									message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC, String.class)
							);
						}

					}
			);

		}

		@Bean
		public KafkaListenerErrorHandler consumeMultiMethodException(MultiListenerBean listener) {
			return (m, e) -> {
				listener.errorLatch.countDown();
				return null;
			};
		}

		@Bean
		public SmartMessageConverter fooContentConverter() {
			return new AbstractMessageConverter(MimeType.valueOf("application/foo")) {

				@Override
				protected boolean supports(Class<?> clazz) {
					return Foo.class.isAssignableFrom(clazz);
				}

				@Override
				@Nullable
				protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
						@Nullable Object conversionHint) {

					return new Foo("bar");
				}

				@Override
				@Nullable
				protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers,
						@Nullable Object conversionHint) {

					return payload instanceof Foo ? ((Foo) payload).getBar() : null;
				}

			};
		}

		@Bean
		List<String> buzProps() {
			return List.of("max.poll.records: 5", "fetch.min.bytes: 123456");
		}

		@Bean
		RecordFilterStrategy<Integer, String> lambdaAll() {
			return rec -> false;
		}

		@Bean
		String barInfo() {
			return "info for the bar listener";
		}

		@Bean
		@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
		ProtoListener proto() {
			return new ProtoListener();
		}

		@Bean
		MultiListenerTwoInstances multiInstanceOne() {
			return new MultiListenerTwoInstances("multiTwoOne");
		}

		@Bean
		MultiListenerTwoInstances multiInstanceTwo() {
			return new MultiListenerTwoInstances("multiTwoTwo");
		}

	}

	static class ProtoListener {

		@KafkaListener(id = "proto", topics = "annotated-42", autoStartup = "false")
		public void listen(String in) {
		}

	}

	@Component
	static class Listener implements ConsumerSeekAware {

		private final ThreadLocal<ConsumerSeekCallback> seekCallBack = new ThreadLocal<>();

		final CountDownLatch latch1 = new CountDownLatch(2);

		final CountDownLatch latch1Batch = new CountDownLatch(2);

		final CountDownLatch latch2 = new CountDownLatch(2); // seek

		final CountDownLatch latch3 = new CountDownLatch(1);

		final CountDownLatch latch4 = new CountDownLatch(1);

		final CountDownLatch latch5 = new CountDownLatch(1);

		final CountDownLatch latch6 = new CountDownLatch(1);

		final CountDownLatch latch7 = new CountDownLatch(1);

		final CountDownLatch latch7a = new CountDownLatch(2);

		volatile String latch7String;

		final CountDownLatch latch8 = new CountDownLatch(1);

		final CountDownLatch latch9 = new CountDownLatch(1);

		final CountDownLatch latch10 = new CountDownLatch(1);

		final CountDownLatch latch11 = new CountDownLatch(1);

		final CountDownLatch latch12 = new CountDownLatch(1);

		final CountDownLatch latch13 = new CountDownLatch(1);

		final CountDownLatch latch14 = new CountDownLatch(1);

		final CountDownLatch latch15 = new CountDownLatch(1);

		final CountDownLatch latch16 = new CountDownLatch(1);

		final CountDownLatch latch17 = new CountDownLatch(4);

		final CountDownLatch latch18 = new CountDownLatch(2);

		final CountDownLatch latch19 = new CountDownLatch(1);

		final CountDownLatch latch20 = new CountDownLatch(1);

		final CountDownLatch latch21 = new CountDownLatch(1);

		final CountDownLatch validationLatch = new CountDownLatch(1);

		final CountDownLatch eventLatch = new CountDownLatch(1);

		final CountDownLatch noLongerIdleEventLatch = new CountDownLatch(1);

		final CountDownLatch keyLatch = new CountDownLatch(1);

		final AtomicBoolean reposition12 = new AtomicBoolean();

		final CountDownLatch projectionLatch = new CountDownLatch(1);

		final CountDownLatch customMethodArgumentResolverLatch = new CountDownLatch(1);

		final CountDownLatch contentLatch = new CountDownLatch(1);

		volatile Integer partition;

		volatile ConsumerRecord<?, ?> capturedRecord;

		volatile Acknowledgment ack;

		volatile Object payload;

		volatile Exception validationException;

		volatile String convertedKey;

		volatile Integer key;

		volatile String topic;

		volatile Foo foo;

		volatile Foo listen16foo;

		volatile ListenerContainerIdleEvent event;

		volatile ListenerContainerNoLongerIdleEvent noLongerIdleEvent;

		volatile List<Integer> keys;

		volatile List<Integer> partitions;

		volatile List<String> topics;

		volatile List<Long> offsets;

		volatile Consumer<?, ?> listen4Consumer;

		volatile Consumer<?, ?> listen12Consumer;

		volatile Consumer<?, ?> listen13Consumer;

		volatile ConsumerRecords<?, ?> consumerRecords;

		volatile String receivedGroupId;

		volatile String listenerInfo;

		volatile String username;

		volatile String name;

		volatile CustomMethodArgument customMethodArgument;

		volatile Foo contentFoo;

		volatile String batchOverrideStackTrace;

		@KafkaListener(id = "manualStart", topics = "manualStart",
				containerFactory = "kafkaAutoStartFalseListenerContainerFactory",
				properties = { ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG + ":301000",
						ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG + ":59000" })
		public void manualStart(String foo) {
		}

		private final AtomicBoolean reposition1 = new AtomicBoolean();

		@KafkaListener(id = "foo", topics = "#{'${topicOne:annotated1,foo}'.split(',')}",
				filter = "lambdaAll", info = "fee fi fo fum")
		public void listen1(String foo, @Header(value = KafkaHeaders.GROUP_ID, required = false) String groupId,
				@Header(KafkaHeaders.LISTENER_INFO) String listenerInfo) {
			this.receivedGroupId = groupId;
			this.listenerInfo = listenerInfo;
			if (this.reposition1.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
			this.latch1.countDown();
		}

		@KafkaListener(id = "fooBatch", topics = "annotated1", batch = "true")
		public void listen1Batch(List<String> foo) {
			StringWriter stringWriter = new StringWriter();
			PrintWriter printWriter = new PrintWriter(stringWriter, true);
			new RuntimeException().printStackTrace(printWriter);
			this.batchOverrideStackTrace = stringWriter.getBuffer().toString();
			foo.forEach(s ->  this.latch1Batch.countDown());
		}

		@KafkaListener(id = "bar", topicPattern = "${topicTwo:annotated2}", filter = "#{@lambdaAll}",
				info = "#{@barInfo}")
		public void listen2(@Payload String foo,
				@Header(KafkaHeaders.GROUP_ID) String groupId,
				@Header(KafkaHeaders.RECEIVED_KEY) Integer key,
				@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
				@Header(KafkaHeaders.LISTENER_INFO) String listenerInfo) {
			this.key = key;
			this.partition = partition;
			this.topic = topic;
			this.receivedGroupId = groupId;
			this.listenerInfo = listenerInfo;
			this.latch2.countDown();
			if (this.latch2.getCount() > 0) {
				this.seekCallBack.get().seek(topic, partition, 0);
			}
		}

		private final AtomicBoolean reposition3 = new AtomicBoolean();

		@KafkaListener(id = "baz", topicPartitions = @TopicPartition(topic = "${topicThree:annotated3}",
				partitions = "${zero:0}"), errorHandler = "listen3ErrorHandler",
				containerFactory = "factoryWithBadConverter")
		public void listen3(ConsumerRecord<?, ?> record) {
			if (this.reposition3.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
			this.capturedRecord = record;
			this.latch3.countDown();
		}

		@KafkaListener(id = "#{'qux'}", topics = "annotated4",
				containerFactory = "kafkaManualAckListenerContainerFactory", containerGroup = "qux#{'Group'}",
				properties = {
						"max.poll.interval.ms:#{'${poll.interval:60000}'}",
						ConsumerConfig.MAX_POLL_RECORDS_CONFIG + "=#{'${poll.recs:100}'}"
				})
		public void listen4(@Payload String foo, Acknowledgment ack, Consumer<?, ?> consumer) {
			this.ack = ack;
			this.ack.acknowledge();
			this.listen4Consumer = consumer;
			this.latch4.countDown();
		}

		@EventListener(condition = "event.listenerId.startsWith('qux')")
		public void eventHandler(ListenerContainerIdleEvent event) {
			this.event = event;
			eventLatch.countDown();
		}

		@EventListener(condition = "event.listenerId.startsWith('qux')")
		public void eventHandler(ListenerContainerNoLongerIdleEvent event) {
			this.noLongerIdleEvent = event;
			noLongerIdleEventLatch.countDown();
		}

		@KafkaListener(id = "fiz", topicPartitions = {
				@TopicPartition(topic = "annotated5", partitions = { "#{'${foo:0,1}'.split(',')}" }),
				@TopicPartition(topic = "annotated6", partitions = "0",
						partitionOffsets = @PartitionOffset(partition = "${xxx:1}", initialOffset = "${yyy:0}",
								relativeToCurrent = "${zzz:true}"))
		}, clientIdPrefix = "${foo.xxx:clientIdViaAnnotation}", properties = "#{'${spel.props}'.split(',')}")
		public void listen5(ConsumerRecord<?, ?> record) {
			this.capturedRecord = record;
			this.latch5.countDown();
		}

		@KafkaListener(id = "buz", topics = "annotated10", containerFactory = "kafkaJsonListenerContainerFactory",
				groupId = "buz.explicitGroupId", properties = "#{@buzProps}")
		public void listen6(Foo foo) {
			this.foo = foo;
			this.latch6.countDown();
		}

		@KafkaListener(id = "jsonHeaders", topics = "annotated31",
				containerFactory = "kafkaJsonListenerContainerFactory",
				groupId = "jsonHeaders")
		public void jsonHeaders(Bar foo) { // should be mapped to Foo via Headers
			this.foo = (Foo) foo;
			this.latch19.countDown();
		}

		@KafkaListener(id = "rebalanceListener", topics = "annotated11", idIsGroup = false,
				containerFactory = "kafkaRebalanceListenerContainerFactory", autoStartup = "${foobarbaz:false}",
				concurrency = "${fixbux:3}")
		public void listen7(@Payload(required = false) String foo) {
			this.latch7String = foo;
			this.latch7.countDown();
			this.latch7a.countDown();
		}

		@KafkaListener(id = "quux", topics = "annotated12")
		public void listen8(@Payload(required = false) String none) {
			assertThat(none).isNull();
			this.latch8.countDown();
		}

		@KafkaListener(id = "corge", topics = "annotated13")
		public void listen9(Object payload) {
			assertThat(payload).isNotNull();
			this.latch9.countDown();
		}

		private final AtomicBoolean reposition10 = new AtomicBoolean();

		@KafkaListener(id = "list1", topics = "annotated14", containerFactory = "batchSpyFactory",
				errorHandler = "listen10ErrorHandler")
		public void listen10(List<String> list, @Header(KafkaHeaders.GROUP_ID) String groupId) {
			if (this.reposition10.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
			this.payload = list;
			this.receivedGroupId = groupId;
			this.latch10.countDown();
		}

		@KafkaListener(id = "list2", topics = "annotated15", containerFactory = "batchFactory",
				info = "info for batch")
		public void listen11(List<String> list,
				@Header(KafkaHeaders.RECEIVED_KEY) List<Integer> keys,
				@Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
				@Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
				@Header(KafkaHeaders.OFFSET) List<Long> offsets,
				@Header(KafkaHeaders.LISTENER_INFO) String info) {
			this.payload = list;
			this.keys = keys;
			this.partitions = partitions;
			this.topics = topics;
			this.offsets = offsets;
			this.listenerInfo = info;
			this.latch11.countDown();
		}

		@KafkaListener(id = "list3", topics = "annotated16", containerFactory = "batchFactory",
				errorHandler = "listen12ErrorHandler")
		public void listen12(List<ConsumerRecord<Integer, String>> list, Consumer<?, ?> consumer) {
			this.payload = list;
			this.listen12Consumer = consumer;
			this.latch12.countDown();
			if (this.reposition12.compareAndSet(false, true)) {
				throw new RuntimeException("reposition");
			}
		}

		@KafkaListener(id = "list4", topics = "annotated17", containerFactory = "batchManualFactory")
		public void listen13(List<ConsumerRecord<Integer, String>> list, Acknowledgment ack, Consumer<?, ?> consumer) {
			this.payload = list;
			this.ack = ack;
			ack.acknowledge();
			this.listen13Consumer = consumer;
			this.latch13.countDown();
		}

		@KafkaListener(id = "list5", topics = "annotated18", containerFactory = "batchFactory")
		public void listen14(List<Message<?>> list) {
			this.payload = list;
			this.latch14.countDown();
		}

		@KafkaListener(id = "list6", topics = "annotated19", containerFactory = "batchManualFactory2")
		public void listen15(List<Message<?>> list, Acknowledgment ack) {
			this.payload = list;
			this.ack = ack;
			ack.acknowledge();
			this.latch15.countDown();
		}

		@KafkaListener(id = "converter", topics = "annotated32", containerFactory = "recordAckListenerContainerFactory",
				groupId = "converter.explicitGroupId")
		public void listen16(Foo foo) {
			this.listen16foo = foo;
			this.latch20.countDown();
		}

		@KafkaListener(id = "errorHandler", topics = "annotated20", errorHandler = "consumeException")
		public String errorHandler(String data) throws Exception {
			throw new Exception("return this");
		}

		@KafkaListener(id = "replyingListener", topics = "annotated21")
		@SendTo("a!{request.value()}") // runtime SpEL - test payload is the reply queue minus leading 'a'
		public String replyingListener(String in) {
			return in.toUpperCase();
		}

		@KafkaListener(id = "replyingBatchListener", topics = "annotated22", containerFactory = "batchFactory")
		@SendTo("a#{'nnotated22reply'}") // config time SpEL
		public Collection<String> replyingBatchListener(List<String> in) {
			return in.stream().map(String::toUpperCase).collect(Collectors.toList());
		}

		@KafkaListener(id = "replyingListenerWithErrorHandler", topics = "annotated23",
				errorHandler = "replyErrorHandler")
		@SendTo("${foo:annotated23reply}")
		public String replyingListenerWithErrorHandler(String in) {
			throw new RuntimeException("return this");
		}

		@KafkaListener(id = "voidListenerWithReplyingErrorHandler", topics = "annotated30",
				errorHandler = "voidSendToErrorHandler")
		@SendTo("annotated30reply")
		public void voidListenerWithReplyingErrorHandler(String in) {
			throw new RuntimeException("fail");
		}

		@KafkaListener(id = "replyingBatchListenerWithErrorHandler", topics = "annotated24",
				containerFactory = "batchFactory", errorHandler = "replyBatchErrorHandler")
		@SendTo("annotated24reply")
		public Collection<String> replyingBatchListenerWithErrorHandler(List<String> in) {
			throw new RuntimeException("return this");
		}

		@KafkaListener(id = "replyingBatchListenerCollection", topics = "annotated38",
				containerFactory = "batchJsonReplyFactory", splitIterables = false)
		@SendTo("annotated38reply")
		public Collection<String> replyingBatchListenerCollection(List<String> in) {
			return in.stream()
					.map(String::toUpperCase)
					.collect(Collectors.toList());
		}

		@KafkaListener(id = "batchAckListener", topics = { "annotated26", "annotated27" },
				containerFactory = "batchFactory")
		public void batchAckListener(@SuppressWarnings("unused") List<String> in,
				@Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitionsHeader,
				@Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topicsHeader,
				Consumer<?, ?> consumer) {

			for (int i = 0; i < topicsHeader.size(); i++) {
				this.latch17.countDown();
				String inTopic = topicsHeader.get(i);
				Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> committed = consumer.committed(
						Collections.singleton(
								new org.apache.kafka.common.TopicPartition(inTopic, partitionsHeader.get(i))));
				if (committed.values().iterator().next() != null) {
					if ("annotated26".equals(inTopic) && committed
								.values()
								.iterator()
								.next()
								.offset() == 1) {
						this.latch18.countDown();
					}
					else if ("annotated27".equals(inTopic) && committed
								.values()
								.iterator()
								.next()
								.offset() == 3) {
						this.latch18.countDown();
					}
				}
			}
		}

		@KafkaListener(id = "ackWithAutoContainer", topics = "annotated28", errorHandler = "badAckConfigErrorHandler",
				containerFactory = "withNoReplyTemplateContainerFactory")
		public void ackWithAutoContainerListener(String payload, Acknowledgment ack) {
			// empty
		}

		@KafkaListener(id = "bytesKey", topics = "annotated36", clientIdPrefix = "tag",
				containerFactory = "bytesStringListenerContainerFactory")
		public void bytesKey(String in, @Header(KafkaHeaders.RECEIVED_KEY) String key) {
			this.convertedKey = key;
			this.keyLatch.countDown();
		}

		@KafkaListener(topics = "annotated29")
		public void anonymousListener(String in) {
		}

		@KafkaListener(id = "pollResults", topics = "annotated34", containerFactory = "batchFactory")
		public void pollResults(ConsumerRecords<?, ?> records) {
			this.consumerRecords = records;
			this.latch21.countDown();
		}

		@KafkaListener(id = "validated", topics = "annotated35", errorHandler = "validationErrorHandler",
				containerFactory = "kafkaJsonListenerContainerFactory")
		public void validatedListener(@Payload @Valid ValidatedClass val) {
		}

		@KafkaListener(id = "projection", topics = "annotated37",
				containerFactory = "projectionListenerContainerFactory")
		public void projectionListener(ProjectionSample sample) {
			this.username = sample.getUsername();
			this.name = sample.getName();
			this.projectionLatch.countDown();
		}

		@KafkaListener(id = "customMethodArgumentResolver", topics = "annotated39")
		public void customMethodArgumentResolverListener(String data, CustomMethodArgument customMethodArgument) {
			this.customMethodArgument = customMethodArgument;
			this.customMethodArgumentResolverLatch.countDown();
		}

		@KafkaListener(id = "smartConverter", topics = "annotated41", contentTypeConverter = "fooContentConverter")
		public void smart(Foo foo) {
			this.contentFoo = foo;
			this.contentLatch.countDown();
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			this.seekCallBack.set(callback);
		}

	}

	static class ProxyListenerPostProcessor implements BeanPostProcessor {

		@Override
		public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
			if ("multiListenerSendTo".equals(beanName)) {
				ProxyFactory proxyFactory = new ProxyFactory(bean);
				proxyFactory.setProxyTargetClass(true);
				proxyFactory.addAdvice(new MethodInterceptor() {
					@Override
					public Object invoke(MethodInvocation invocation) throws Throwable {
						logger.info(String.format("Proxy listener for %s.$s",
								invocation.getMethod().getDeclaringClass(), invocation.getMethod().getName()));
						return invocation.proceed();
					}
				});
				return proxyFactory.getProxy();
			}
			return bean;
		}
	}

	public static class SeekToLastOnIdleListener extends AbstractConsumerSeekAware {

		final CountDownLatch latch1 = new CountDownLatch(10);

		final CountDownLatch latch2 = new CountDownLatch(12);

		final CountDownLatch latch3 = new CountDownLatch(13);

		final CountDownLatch latch4 = new CountDownLatch(2);

		final AtomicInteger idleRewinds = new AtomicInteger();

		final Semaphore semaphore = new Semaphore(0);

		final Set<Thread> consumerThreads = ConcurrentHashMap.newKeySet();

		@KafkaListener(id = "seekOnIdle", topics = "seekOnIdle", autoStartup = "false", concurrency = "2",
				clientIdPrefix = "seekOnIdle", containerFactory = "kafkaManualAckListenerContainerFactory")
		public void listen(@SuppressWarnings("unused") String in, Acknowledgment ack) {
			this.latch3.countDown();
			this.latch2.countDown();
			this.latch1.countDown();
			ack.acknowledge();
			this.semaphore.release();
		}

		@Override
		public void onIdleContainer(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
				ConsumerSeekCallback callback) {

			if (this.semaphore.availablePermits() > 0 && this.idleRewinds.getAndIncrement() < 10) {
				try {
					this.semaphore.acquire();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				assignments.keySet().forEach(tp -> callback.seekRelative(tp.topic(), tp.partition(), -1, true));
			}
		}

		public void rewindAllOneRecord() {
			getSeekCallbacks()
				.forEach((tp, callback) ->
					callback.seekRelative(tp.topic(), tp.partition(), -1, true));
		}

		public void rewindOnePartitionOneRecord(String topic, int partition) {
			getSeekCallbackFor(new org.apache.kafka.common.TopicPartition(topic, partition))
				.seekRelative(topic, partition, -1, true);
		}

		@Override
		public synchronized void onPartitionsAssigned(Map<org.apache.kafka.common.TopicPartition, Long> assignments,
				ConsumerSeekCallback callback) {

			super.onPartitionsAssigned(assignments, callback);
			if (assignments.size() > 0) {
				this.consumerThreads.add(Thread.currentThread());
				notifyAll();
			}
		}

		@Override
		public void onPartitionsRevoked(Collection<org.apache.kafka.common.TopicPartition> partitions) {
			super.onPartitionsRevoked(partitions);
			if (partitions.isEmpty()) {
				this.latch4.countDown();
			}
		}

		public synchronized void waitForBalancedAssignment() throws InterruptedException {
			int n = 0;
			while (this.consumerThreads.size() < 2) {
				wait(1000);
				if (n++ > 20) {
					throw new IllegalStateException("Balanced distribution did not occur");
				}
			}
		}

	}

	interface IfaceListener<T> {

		void listen(T foo);

		@SendTo("annotated43reply")
		@KafkaListener(id = "ifcR", topics = "annotated43")
		String reply(String in);

	}

	static class IfaceListenerImpl implements IfaceListener<String> {

		private final CountDownLatch latch1 = new CountDownLatch(1);

		private final CountDownLatch latch2 = new CountDownLatch(1);

		@Override
		@KafkaListener(id = "ifc", topics = "annotated7")
		public void listen(String foo) {
			latch1.countDown();
		}

		@KafkaListener(id = "ifctx", topics = "annotated9")
		@Transactional
		public void listenTx(String foo) {
			latch2.countDown();
		}

		@Override
		public String reply(String in) {
			return in.toUpperCase();
		}

		public CountDownLatch getLatch1() {
			return latch1;
		}

		public CountDownLatch getLatch2() {
			return latch2;
		}

	}

	@KafkaListener(id = "multi", topics = "annotated8", errorHandler = "consumeMultiMethodException")
	static class MultiListenerBean {

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(1);

		final CountDownLatch errorLatch = new CountDownLatch(1);

		volatile ConsumerRecordMetadata meta;

		@KafkaHandler
		public void bar(@NonNull String bar, @Header(KafkaHeaders.RECORD_METADATA) ConsumerRecordMetadata meta) {
			if ("junk".equals(bar)) {
				throw new RuntimeException("intentional");
			}
			else {
				this.meta = meta;
				this.latch1.countDown();
			}
		}

		@KafkaHandler
		@SendTo("#{'${foo:annotated8reply}'}")
		public String bar(@Payload(required = false) KafkaNull nul, @Header(KafkaHeaders.RECEIVED_KEY) int key) {
			this.latch2.countDown();
			return "OK";
		}

		public void foo(String bar) {
		}

	}

	@KafkaListener(id = "multiJson", topics = "annotated33", containerFactory = "kafkaJsonListenerContainerFactory2")
	static class MultiJsonListenerBean {

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(1);

		final CountDownLatch latch3 = new CountDownLatch(1);

		final CountDownLatch latch4 = new CountDownLatch(1);

		volatile Foo foo;

		volatile Baz baz;

		volatile Bar bar;

		volatile ValidatedClass validated;

		@KafkaHandler
		public void bar(Foo foo) {
			this.foo = foo;
			this.latch1.countDown();
		}

		@KafkaHandler
		public void bar(Baz baz) {
			this.baz = baz;
			this.latch2.countDown();
		}

		@KafkaHandler
		public void bar(@Valid ValidatedClass val) {
			this.validated = val;
			this.latch4.countDown();
		}

		@KafkaHandler(isDefault = true)
		public void defaultHandler(Bar bar) {
			this.bar = bar;
			this.latch3.countDown();
		}

	}

	@KafkaListener(id = "multiNoDefault", topics = "annotated40", containerFactory = "kafkaJsonListenerContainerFactory2")
	static class MultiListenerNoDefault {

		final CountDownLatch latch1 = new CountDownLatch(1);

		volatile ValidatedClass validated;

		@KafkaHandler
		public void bar(@Valid ValidatedClass val) {
			this.validated = val;
			this.latch1.countDown();
		}

	}

	@KafkaListener(id = "multiSendTo", topics = "annotated25")
	@SendTo("annotated25reply1")
	interface MultiListenerSendTo {

		@KafkaHandler
		String foo(String in);

		@KafkaHandler
		@SendTo("!{'annotated25reply2'}")
		String bar(KafkaNull nul, int key);

	}

	static class MultiListenerSendToImpl implements MultiListenerSendTo {

		@Override
		public String foo(String in) {
			return in.toUpperCase();
		}

		@Override
		public String bar(@Payload(required = false) KafkaNull nul,
				@Header(KafkaHeaders.RECEIVED_KEY) int key) {
			return "BAR";
		}

	}

	@KafkaListener(id = "#{__listener.id}", topics = "multiWithTwoInstances", autoStartup = "false")
	static class MultiListenerTwoInstances {

		private final String id;

		MultiListenerTwoInstances(String id) {
			this.id = id;
		}

		public String getId() {
			return this.id;
		}

		@KafkaHandler
		void listen(String in) {
		}

	}

	public interface Bar {

		String getBar();

	}

	public static class Foo implements Bar {

		private String bar;


		public Foo() {
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		@Override
		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((this.bar == null) ? 0 : this.bar.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Foo other = (Foo) obj;
			if (this.bar == null) {
				if (other.bar != null) {
					return false;
				}
			}
			else if (!this.bar.equals(other.bar)) {
				return false;
			}
			return true;
		}

	}

	public static class Baz implements Bar {

		private String bar;

		public Baz() {
		}

		public Baz(String bar) {
			this.bar = bar;
		}

		@Override
		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class Qux implements Bar {

		private String bar;

		public Qux() {
		}

		public Qux(String bar) {
			this.bar = bar;
		}

		@Override
		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

	}

	public static class RecordPassAllFilter implements RecordFilterStrategy<Integer, CharSequence> {

		boolean called;

		boolean batchCalled;

		@Override
		public boolean filter(ConsumerRecord<Integer, CharSequence> consumerRecord) {
			this.called = true;
			return false;
		}

		@Override
		public List<ConsumerRecord<Integer, CharSequence>> filterBatch(
				List<ConsumerRecord<Integer, CharSequence>> records) {

			this.batchCalled = true;
			return records;
		}


	}

	public static class FooConverter implements Converter<String, Foo> {

		private Converter<String, Foo> delegate;

		public Converter<String, Foo> getDelegate() {
			return delegate;
		}

		public void setDelegate(
				Converter<String, Foo> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Foo convert(String source) {
			return delegate.convert(source);
		}

	}

	public static class ValidatedClass {

		@Max(10)
		private int bar;

		private volatile boolean validated;

		volatile int valCount;

		public ValidatedClass() {
		}

		public ValidatedClass(int bar) {
			this.bar = bar;
		}

		public int getBar() {
			return this.bar;
		}

		public void setBar(int bar) {
			this.bar = bar;
		}

		public boolean isValidated() {
			return this.validated;
		}

		public void setValidated(boolean validated) {
			this.validated = validated;
			if (validated) { // don't count the json deserialization call
				this.valCount++;
			}
		}

	}

	interface ProjectionSample {

		String getUsername();

		@JsonPath("$.user.name")
		String getName();

	}

	static class CustomMethodArgument {

		final String body;

		final String topic;

		CustomMethodArgument(String body, String topic) {
			this.body = body;
			this.topic = topic;
		}

	}


}
