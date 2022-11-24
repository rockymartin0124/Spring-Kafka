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

package org.springframework.kafka.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.streams.KafkaStreamsMicrometerListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * @author Nurettin Yilmaz
 * @author Artem Bilan
 *
 * @since 2.1.5
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
public class KafkaStreamsCustomizerTests {

	private static final String APPLICATION_ID = "testStreams";

	private static final TestStateListener STATE_LISTENER = new TestStateListener();

	@Autowired
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@Autowired
	private KafkaStreamsConfig config;

	@Autowired
	private MeterRegistry meterRegistry;

	@Test
	public void testKafkaStreamsCustomizer(@Autowired KafkaStreamsConfiguration configuration,
			@Autowired KafkaStreamsConfig config) {

		KafkaStreams.State state = this.streamsBuilderFactoryBean.getKafkaStreams().state();
		assertThat(STATE_LISTENER.getCurrentState()).isEqualTo(state);
		Properties properties = configuration.asProperties();
		assertThat(properties.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
				.isEqualTo(Collections.singletonList(config.broker.getBrokersAsString()));
		assertThat(properties.get(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG))
				.isEqualTo(Foo.class);
		assertThat(properties.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG))
				.isEqualTo(1000);
		assertThat(this.config.builderConfigured.get()).isTrue();
		assertThat(this.config.topologyConfigured.get()).isTrue();
		assertThat(this.meterRegistry.get("kafka.consumer.coordinator.join.total")
				.tag("customTag", "stream")
				.tag("spring.id", KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
				.functionCounter()
				.count())
					.isGreaterThanOrEqualTo(0);
		assertThat(this.meterRegistry.get("kafka.producer.incoming.byte.total")
				.tag("customTag", "stream")
				.tag("spring.id", KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
				.functionCounter()
				.count())
					.isGreaterThanOrEqualTo(0);
	}

	@Configuration
	@EnableKafka
	@EnableKafkaStreams
	public static class KafkaStreamsConfig {

		final AtomicBoolean builderConfigured = new AtomicBoolean();

		final AtomicBoolean topologyConfigured = new AtomicBoolean();

		@Autowired
		EmbeddedKafkaBroker broker;

		@SuppressWarnings("unchecked")
		@Bean
		public MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
		public StreamsBuilderFactoryBean defaultKafkaStreamsBuilder() {
			StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean(kStreamsConfigs());
			streamsBuilderFactoryBean.setKafkaStreamsCustomizer(customizer());
			streamsBuilderFactoryBean.setInfrastructureCustomizer(new KafkaStreamsInfrastructureCustomizer() {

				@SuppressWarnings("unchecked")
				@Override
				public void configureBuilder(StreamsBuilder builder) {
					KafkaStreamsConfig.this.builderConfigured.set(true);
					StoreBuilder<?> storeBuilder = Stores.keyValueStoreBuilder(
							Stores.persistentKeyValueStore("testStateStore"),
							Serdes.Integer(),
							Serdes.String());
					builder.addStateStore(storeBuilder);
				}

				@Override
				public void configureTopology(Topology topology) {
					KafkaStreamsConfig.this.topologyConfigured.set(true);
				}

			});
			streamsBuilderFactoryBean.addListener(new KafkaStreamsMicrometerListener(meterRegistry(),
					Collections.singletonList(new ImmutableTag("customTag", "stream"))));
			return streamsBuilderFactoryBean;
		}

		@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
		public KafkaStreamsConfiguration kStreamsConfigs() {
			Map<String, Object> props = new HashMap<>();
			props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
			props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
					Collections.singletonList(this.broker.getBrokersAsString()));
			props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, Foo.class);
			props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1000);
			return new KafkaStreamsConfiguration(props);
		}

		private KafkaStreamsCustomizer customizer() {
			return kafkaStreams -> kafkaStreams.setStateListener(STATE_LISTENER);
		}

		@Bean
		public KStream<String, String> testStream(StreamsBuilder kStreamBuilder) {
			KStream<String, String> stream = kStreamBuilder.stream("test_topic");

			stream
					.process(() -> new ContextualProcessor<String, String, String, String>() {

						@Override
						public void process(Record<String, String> record) {
						}

					}, "testStateStore")
					.to("test_output");

			return stream;
		}
	}

	static class TestStateListener implements KafkaStreams.StateListener {

		private KafkaStreams.State currentState;

		@Override
		public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
			this.currentState = newState;
		}

		KafkaStreams.State getCurrentState() {
			return this.currentState;
		}

	}

	public static class Foo implements DeserializationExceptionHandler {

		@Override
		public void configure(Map<String, ?> configs) {

		}

		@Override
		public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record,
				Exception exception) {
			return null;
		}

	}

}
