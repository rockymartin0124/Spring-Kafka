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

package org.springframework.kafka.streams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Artem Bilan
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Elliot Metsger
 * @author Zach Olauson
 *
 * @since 1.1.4
 */
@SpringJUnitConfig
@DirtiesContext
@TestPropertySource(properties = "streaming.topic.two=streamingTopic2")
@EmbeddedKafka(partitions = 1,
		topics = {
				KafkaStreamsTests.STREAMING_TOPIC1,
				"${streaming.topic.two}",
				KafkaStreamsTests.FOOS },
		brokerProperties = {
				"auto.create.topics.enable=${topics.autoCreate:false}",
				"delete.topic.enable=${topic.delete:true}" },
		brokerPropertiesLocation = "classpath:/${broker.filename:broker}.properties")
public class KafkaStreamsTests {

	static final String STREAMING_TOPIC1 = "streamingTopic1";

	static final String FOOS = "foos";

	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	private CompletableFuture<ConsumerRecord<?, String>> resultFuture;

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Value("${streaming.topic.two}")
	private String streamingTopic2;

	@Autowired
	private AtomicBoolean stateChangeCalled;

	@SuppressWarnings("deprecation")
	@Test
	public void testKStreams() throws Exception {
		assertThat(this.embeddedKafka.getKafkaServer(0).config().autoCreateTopicsEnable()).isFalse();
		assertThat(this.embeddedKafka.getKafkaServer(0).config().deleteTopicEnable()).isTrue();
		assertThat(this.embeddedKafka.getKafkaServer(0).config().brokerId()).isEqualTo(2);

		this.streamsBuilderFactoryBean.stop();

		CountDownLatch stateLatch = new CountDownLatch(1);

		this.streamsBuilderFactoryBean.setStateListener((newState, oldState) -> stateLatch.countDown());
		StreamsUncaughtExceptionHandler exceptionHandler = mock(StreamsUncaughtExceptionHandler.class);
		this.streamsBuilderFactoryBean.setStreamsUncaughtExceptionHandler(exceptionHandler);

		this.streamsBuilderFactoryBean.start();

		String payload = "foo" + UUID.randomUUID().toString();
		String payload2 = "foo" + UUID.randomUUID().toString();

		this.kafkaTemplate.sendDefault(0, payload);
		this.kafkaTemplate.sendDefault(0, payload2);
		this.kafkaTemplate.flush();

		ConsumerRecord<?, String> result = resultFuture.get(600, TimeUnit.SECONDS);

		assertThat(result).isNotNull();

		assertThat(result.topic()).isEqualTo(streamingTopic2);
		assertThat(result.value()).isEqualTo(payload.toUpperCase() + payload2.toUpperCase());
		assertThat(result.headers().lastHeader("foo")).isNotNull();
		assertThat(result.headers().lastHeader("foo").value()).isEqualTo("bar".getBytes());
		assertThat(result.headers().lastHeader("spel")).isNotNull();

		assertThat(stateLatch.await(10, TimeUnit.SECONDS)).isTrue();

		KafkaStreams kafkaStreams = this.streamsBuilderFactoryBean.getKafkaStreams();

		assertThat(KafkaTestUtils.getPropertyValue(kafkaStreams, "streamsUncaughtExceptionHandler.arg$2"))
				.isSameAs(exceptionHandler);
		assertThat(this.stateChangeCalled.get()).isTrue();
	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfig {

		@Value("${" + EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS + "}")
		private String brokerAddresses;

		@Value("${streaming.topic.two}")
		private String streamingTopic2;

		@Bean
		public ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(producerConfigs());
		}

		@Bean
		public Map<String, Object> producerConfigs() {
			return KafkaTestUtils.producerProps(this.brokerAddresses);
		}

		@Bean
		public KafkaTemplate<Integer, String> template() {
			KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(producerFactory(), true);
			kafkaTemplate.setDefaultTopic(STREAMING_TOPIC1);
			return kafkaTemplate;
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerAddresses);
			props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
			props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
			props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
					WallclockTimestampExtractor.class.getName());
			props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "100");
			return new KafkaStreamsConfiguration(props);
		}

		@Bean
		public AtomicBoolean stateChangeCalled() {
			return new AtomicBoolean();
		}

		@Bean
		public StreamsBuilderFactoryBeanConfigurer customizer() {
			return fb -> fb.setStateListener((newState, oldState) -> {
				stateChangeCalled().set(true);
			});
		}

		@Bean
		public KStream<Integer, String> kStream(StreamsBuilder kStreamBuilder) {
			KStream<Integer, String> stream = kStreamBuilder.stream(STREAMING_TOPIC1);
			Map<String, Expression> headers = new HashMap<>();
			headers.put("foo", new LiteralExpression("bar"));
			SpelExpressionParser parser = new SpelExpressionParser();
			headers.put("spel", parser.parseExpression("context.timestamp() + key + value"));
			stream.mapValues((ValueMapper<String, String>) String::toUpperCase)
					.mapValues(Foo::new)
					.repartition(Repartitioned.with(Serdes.Integer(), new JsonSerde<Foo>() { }))
					.mapValues(Foo::getName)
					.groupByKey()
					.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(1000)))
					.reduce((value1, value2) -> value1 + value2, Materialized.as("windowStore"))
					.toStream()
					.map((windowedId, value) -> new KeyValue<>(windowedId.key(), value))
					.filter((i, s) -> s.length() > 40)
					.process(() -> new HeaderEnricherProcessor<>(headers))
					.to(streamingTopic2);

			stream.print(Printed.toSysOut());

			return stream;
		}

		@Bean
		public Map<String, Object> consumerConfigs() {
			Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(this.brokerAddresses, "testGroup",
					"false");
			return consumerProps;
		}

		@Bean
		public ConsumerFactory<Integer, String> consumerFactory() {
			return new DefaultKafkaConsumerFactory<>(consumerConfigs());
		}

		@Bean
		public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
					kafkaListenerContainerFactory() {

			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

		@Bean
		public CompletableFuture<ConsumerRecord<?, String>> resultFuture() {
			return new CompletableFuture<>();
		}

		@KafkaListener(topics = "${streaming.topic.two}")
		public void listener(ConsumerRecord<?, String> payload) {
			resultFuture().complete(payload);
		}

	}

	static class Foo {

		private String name;

		Foo() {
		}

		Foo(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

	}

}
