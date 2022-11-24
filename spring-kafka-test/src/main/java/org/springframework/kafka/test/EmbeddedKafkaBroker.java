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

package org.springframework.kafka.test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerState;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.test.core.BrokerAddress;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.CoreUtils;
import kafka.utils.TestUtils;
import kafka.zk.ZkFourLetterWords;
import kafka.zookeeper.ZooKeeperClient;

/**
 * An embedded Kafka Broker(s) and Zookeeper manager.
 * This class is intended to be used in the unit tests.
 *
 * @author Marius Bogoevici
 * @author Artem Bilan
 * @author Gary Russell
 * @author Kamill Sokol
 * @author Elliot Kennedy
 * @author Nakul Mishra
 * @author Pawel Lozinski
 * @author Adrian Chlebosz
 *
 * @since 2.2
 */
public class EmbeddedKafkaBroker implements InitializingBean, DisposableBean {

	private static final String BROKER_NEEDED = "Broker must be started before this method can be called";

	private static final String LOOPBACK = "127.0.0.1";

	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(EmbeddedKafkaBroker.class)); // NOSONAR

	public static final String BEAN_NAME = "embeddedKafka";

	public static final String SPRING_EMBEDDED_KAFKA_BROKERS = "spring.embedded.kafka.brokers";

	public static final String SPRING_EMBEDDED_ZOOKEEPER_CONNECT = "spring.embedded.zookeeper.connect";

	/**
	 * Set the value of this property to a property name that should be set to the list of
	 * embedded broker addresses instead of {@value #SPRING_EMBEDDED_KAFKA_BROKERS}.
	 */
	public static final String BROKER_LIST_PROPERTY = "spring.embedded.kafka.brokers.property";

	public static final int DEFAULT_ADMIN_TIMEOUT = 10;

	public static final int DEFAULT_ZK_SESSION_TIMEOUT = 18000;

	public static final int DEFAULT_ZK_CONNECTION_TIMEOUT = DEFAULT_ZK_SESSION_TIMEOUT;

	private static final Method GET_BROKER_STATE_METHOD;

	static {
		try {
			Method method = KafkaServer.class.getDeclaredMethod("brokerState");
			if (method.getReturnType().equals(AtomicReference.class)) {
				GET_BROKER_STATE_METHOD = method;
			}
			else {
				GET_BROKER_STATE_METHOD = null;
			}
		}
		catch (NoSuchMethodException | SecurityException e) {
			throw new IllegalStateException("Failed to determine KafkaServer.brokerState() method; client version: "
					+ AppInfoParser.getVersion(), e);
		}
	}

	private final int count;

	private final boolean controlledShutdown;

	private final Set<String> topics;

	private final int partitionsPerTopic;

	private final List<KafkaServer> kafkaServers = new ArrayList<>();

	private final Map<String, Object> brokerProperties = new HashMap<>();

	private EmbeddedZookeeper zookeeper;

	private String zkConnect;

	private int zkPort;

	private int[] kafkaPorts;

	private Duration adminTimeout = Duration.ofSeconds(DEFAULT_ADMIN_TIMEOUT);

	private int zkConnectionTimeout = DEFAULT_ZK_CONNECTION_TIMEOUT;

	private int zkSessionTimeout = DEFAULT_ZK_SESSION_TIMEOUT;

	private String brokerListProperty;

	private volatile ZooKeeperClient zooKeeperClient;

	public EmbeddedKafkaBroker(int count) {
		this(count, false);
	}

	/**
	 * Create embedded Kafka brokers.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param topics the topics to create (2 partitions per).
	 */
	public EmbeddedKafkaBroker(int count, boolean controlledShutdown, String... topics) {
		this(count, controlledShutdown, 2, topics);
	}

	/**
	 * Create embedded Kafka brokers listening on random ports.
	 * @param count the number of brokers.
	 * @param controlledShutdown passed into TestUtils.createBrokerConfig.
	 * @param partitions partitions per topic.
	 * @param topics the topics to create.
	 */
	public EmbeddedKafkaBroker(int count, boolean controlledShutdown, int partitions, String... topics) {
		this.count = count;
		this.kafkaPorts = new int[this.count]; // random ports by default.
		this.controlledShutdown = controlledShutdown;
		if (topics != null) {
			this.topics = new HashSet<>(Arrays.asList(topics));
		}
		else {
			this.topics = new HashSet<>();
		}
		this.partitionsPerTopic = partitions;
	}

	/**
	 * Specify the properties to configure Kafka Broker before start, e.g.
	 * {@code auto.create.topics.enable}, {@code transaction.state.log.replication.factor} etc.
	 * @param properties the properties to use for configuring Kafka Broker(s).
	 * @return this for chaining configuration.
	 * @see KafkaConfig
	 */
	public EmbeddedKafkaBroker brokerProperties(Map<String, String> properties) {
		this.brokerProperties.putAll(properties);
		return this;
	}

	/**
	 * Specify a broker property.
	 * @param property the property name.
	 * @param value the value.
	 * @return the {@link EmbeddedKafkaBroker}.
	 */
	public EmbeddedKafkaBroker brokerProperty(String property, Object value) {
		this.brokerProperties.put(property, value);
		return this;
	}

	/**
	 * Set explicit ports on which the kafka brokers will listen. Useful when running an
	 * embedded broker that you want to access from other processes.
	 * @param ports the ports.
	 * @return the {@link EmbeddedKafkaBroker}.
	 */
	public EmbeddedKafkaBroker kafkaPorts(int... ports) {
		Assert.isTrue(ports.length == this.count, "A port must be provided for each instance ["
				+ this.count + "], provided: " + Arrays.toString(ports) + ", use 0 for a random port");
		this.kafkaPorts = Arrays.copyOf(ports, ports.length);
		return this;
	}

	/**
	 * Set the system property with this name to the list of broker addresses.
	 * @param brokerListProperty the brokerListProperty to set
	 * @return this broker.
	 * @since 2.3
	 */
	public EmbeddedKafkaBroker brokerListProperty(String brokerListProperty) {
		this.brokerListProperty = brokerListProperty;
		return this;
	}

	/**
	 * Set an explicit port for the embedded Zookeeper.
	 * @param port the port.
	 * @return the {@link EmbeddedKafkaBroker}.
	 * @since 2.3
	 */
	public EmbeddedKafkaBroker zkPort(int port) {
		this.zkPort = port;
		return this;
	}

	/**
	 * Get the port that the embedded Zookeeper is running on or will run on.
	 * @return the port.
	 * @since 2.3
	 */
	public int getZkPort() {
		return this.zookeeper != null ? this.zookeeper.getPort() : this.zkPort;
	}

	/**
	 * Set the port to run the embedded Zookeeper on (default random).
	 * @param zkPort the port.
	 * @since 2.3
	 */
	public void setZkPort(int zkPort) {
		this.zkPort = zkPort;
	}

	/**
	 * Set the timeout in seconds for admin operations (e.g. topic creation, close).
	 * @param adminTimeout the timeout.
	 * @return the {@link EmbeddedKafkaBroker}
	 * @since 2.8.5
	 */
	public EmbeddedKafkaBroker adminTimeout(int adminTimeout) {
		this.adminTimeout = Duration.ofSeconds(adminTimeout);
		return this;
	}

	/**
	 * Set the timeout in seconds for admin operations (e.g. topic creation, close).
	 * Default 10 seconds.
	 * @param adminTimeout the timeout.
	 * @since 2.2
	 */
	public void setAdminTimeout(int adminTimeout) {
		this.adminTimeout = Duration.ofSeconds(adminTimeout);
	}

	/**
	 * Set connection timeout for the client to the embedded Zookeeper.
	 * @param zkConnectionTimeout the connection timeout,
	 * @return the {@link EmbeddedKafkaBroker}.
	 * @since 2.4
	 */
	public synchronized EmbeddedKafkaBroker zkConnectionTimeout(int zkConnectionTimeout) {
		this.zkConnectionTimeout = zkConnectionTimeout;
		return this;
	}

	/**
	 * Set session timeout for the client to the embedded Zookeeper.
	 * @param zkSessionTimeout the session timeout.
	 * @return the {@link EmbeddedKafkaBroker}.
	 * @since 2.4
	 */
	public synchronized EmbeddedKafkaBroker zkSessionTimeout(int zkSessionTimeout) {
		this.zkSessionTimeout = zkSessionTimeout;
		return this;
	}

	@Override
	public void afterPropertiesSet() {
		overrideExitMethods();
		try {
			this.zookeeper = new EmbeddedZookeeper(this.zkPort);
		}
		catch (IOException | InterruptedException e) {
			throw new IllegalStateException("Failed to create embedded Zookeeper", e);
		}
		this.zkConnect = LOOPBACK + ":" + this.zookeeper.getPort();
		this.kafkaServers.clear();
		boolean userLogDir = this.brokerProperties.get(KafkaConfig.LogDirProp()) != null && this.count == 1;
		for (int i = 0; i < this.count; i++) {
			Properties brokerConfigProperties = createBrokerProperties(i);
			brokerConfigProperties.setProperty(KafkaConfig.ReplicaSocketTimeoutMsProp(), "1000");
			brokerConfigProperties.setProperty(KafkaConfig.ControllerSocketTimeoutMsProp(), "1000");
			brokerConfigProperties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp(), "1");
			brokerConfigProperties.setProperty(KafkaConfig.ReplicaHighWatermarkCheckpointIntervalMsProp(),
					String.valueOf(Long.MAX_VALUE));
			this.brokerProperties.forEach(brokerConfigProperties::put);
			if (!this.brokerProperties.containsKey(KafkaConfig.NumPartitionsProp())) {
				brokerConfigProperties.setProperty(KafkaConfig.NumPartitionsProp(), "" + this.partitionsPerTopic);
			}
			if (!userLogDir) {
				logDir(brokerConfigProperties);
			}
			KafkaServer server = TestUtils.createServer(new KafkaConfig(brokerConfigProperties), Time.SYSTEM);
			this.kafkaServers.add(server);
			if (this.kafkaPorts[i] == 0) {
				this.kafkaPorts[i] = TestUtils.boundPort(server, SecurityProtocol.PLAINTEXT);
			}
		}
		createKafkaTopics(this.topics);
		if (this.brokerListProperty == null) {
			this.brokerListProperty = System.getProperty(BROKER_LIST_PROPERTY);
		}
		if (this.brokerListProperty == null) {
			this.brokerListProperty = SPRING_EMBEDDED_KAFKA_BROKERS;
		}
		System.setProperty(this.brokerListProperty, getBrokersAsString());
		System.setProperty(SPRING_EMBEDDED_ZOOKEEPER_CONNECT, getZookeeperConnectionString());
	}

	private void logDir(Properties brokerConfigProperties) {
		try {
			brokerConfigProperties.put(KafkaConfig.LogDirProp(),
					Files.createTempDirectory("spring.kafka." + UUID.randomUUID()).toString());
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void overrideExitMethods() {
		String exitMsg = "Exit.%s(%d, %s) called";
		Exit.setExitProcedure((statusCode, message) -> {
			if (logger.isDebugEnabled()) {
				logger.debug(new RuntimeException(), String.format(exitMsg, "exit", statusCode, message));
			}
			else {
				logger.warn(String.format(exitMsg, "exit", statusCode, message));
			}
		});
		Exit.setHaltProcedure((statusCode, message) -> {
			if (logger.isDebugEnabled()) {
				logger.debug(new RuntimeException(), String.format(exitMsg, "halt", statusCode, message));
			}
			else {
				logger.warn(String.format(exitMsg, "halt", statusCode, message));
			}
		});
	}

	private Properties createBrokerProperties(int i) {
		return TestUtils.createBrokerConfig(i, this.zkConnect, this.controlledShutdown,
				true, this.kafkaPorts[i],
				scala.Option.apply(null),
				scala.Option.apply(null),
				scala.Option.apply(null),
				true, false, 0, false, 0, false, 0, scala.Option.apply(null), 1, false,
				this.partitionsPerTopic, (short) this.count);
	}

	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 */
	public void addTopics(String... topicsToAdd) {
		Assert.notNull(this.zookeeper, BROKER_NEEDED);
		HashSet<String> set = new HashSet<>(Arrays.asList(topicsToAdd));
		createKafkaTopics(set);
		this.topics.addAll(set);
	}

	/**
	 * Add topics to the existing broker(s).
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @since 2.2
	 */
	public void addTopics(NewTopic... topicsToAdd) {
		Assert.notNull(this.zookeeper, BROKER_NEEDED);
		for (NewTopic topic : topicsToAdd) {
			Assert.isTrue(this.topics.add(topic.name()), () -> "topic already exists: " + topic);
			Assert.isTrue(topic.replicationFactor() <= this.count
							&& (topic.replicasAssignments() == null
							|| topic.replicasAssignments().size() <= this.count),
					() -> "Embedded kafka does not support the requested replication factor: " + topic);
		}

		doWithAdmin(admin -> createTopics(admin, Arrays.asList(topicsToAdd)));
	}

	/**
	 * Create topics in the existing broker(s) using the configured number of partitions.
	 * @param topicsToCreate the topics.
	 */
	private void createKafkaTopics(Set<String> topicsToCreate) {
		doWithAdmin(admin -> {
			createTopics(admin,
					topicsToCreate.stream()
						.map(t -> new NewTopic(t, this.partitionsPerTopic, (short) this.count))
						.collect(Collectors.toList()));
		});
	}

	private void createTopics(AdminClient admin, List<NewTopic> newTopics) {
		CreateTopicsResult createTopics = admin.createTopics(newTopics);
		try {
			createTopics.all().get(this.adminTimeout.getSeconds(), TimeUnit.SECONDS);
		}
		catch (Exception e) {
			throw new KafkaException(e);
		}
	}

	/**
	 * Add topics to the existing broker(s) using the configured number of partitions.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @return the results; null values indicate success.
	 * @since 2.5.4
	 */
	public Map<String, Exception> addTopicsWithResults(String... topicsToAdd) {
		Assert.notNull(this.zookeeper, BROKER_NEEDED);
		HashSet<String> set = new HashSet<>(Arrays.asList(topicsToAdd));
		this.topics.addAll(set);
		return createKafkaTopicsWithResults(set);
	}

	/**
	 * Add topics to the existing broker(s) and returning a map of results.
	 * The broker(s) must be running.
	 * @param topicsToAdd the topics.
	 * @return the results; null values indicate success.
	 * @since 2.5.4
	 */
	public Map<String, Exception> addTopicsWithResults(NewTopic... topicsToAdd) {
		Assert.notNull(this.zookeeper, BROKER_NEEDED);
		for (NewTopic topic : topicsToAdd) {
			Assert.isTrue(this.topics.add(topic.name()), () -> "topic already exists: " + topic);
			Assert.isTrue(topic.replicationFactor() <= this.count
							&& (topic.replicasAssignments() == null
							|| topic.replicasAssignments().size() <= this.count),
					() -> "Embedded kafka does not support the requested replication factor: " + topic);
		}

		return doWithAdminFunction(admin -> createTopicsWithResults(admin, Arrays.asList(topicsToAdd)));
	}

	/**
	 * Create topics in the existing broker(s) using the configured number of partitions
	 * and returning a map of results.
	 * @param topicsToCreate the topics.
	 * @return the results; null values indicate success.
	 * @since 2.5.4
	 */
	private Map<String, Exception> createKafkaTopicsWithResults(Set<String> topicsToCreate) {
		return doWithAdminFunction(admin -> {
			return createTopicsWithResults(admin,
					topicsToCreate.stream()
						.map(t -> new NewTopic(t, this.partitionsPerTopic, (short) this.count))
						.collect(Collectors.toList()));
		});
	}

	private Map<String, Exception> createTopicsWithResults(AdminClient admin, List<NewTopic> newTopics) {
		CreateTopicsResult createTopics = admin.createTopics(newTopics);
		Map<String, Exception> results = new HashMap<>();
		createTopics.values()
				.entrySet()
				.stream()
				.map(entry -> {
					Exception result;
					try {
						entry.getValue().get(this.adminTimeout.getSeconds(), TimeUnit.SECONDS);
						result = null;
					}
					catch (InterruptedException | ExecutionException | TimeoutException e) {
						result = e;
					}
					return new SimpleEntry<>(entry.getKey(), result);
				})
				.forEach(entry -> results.put(entry.getKey(), entry.getValue()));
		return results;
	}

	/**
	 * Create an {@link AdminClient}; invoke the callback and reliably close the admin.
	 * @param callback the callback.
	 */
	public void doWithAdmin(java.util.function.Consumer<AdminClient> callback) {
		Map<String, Object> adminConfigs = new HashMap<>();
		adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokersAsString());
		try (AdminClient admin = AdminClient.create(adminConfigs)) {
			callback.accept(admin);
		}
	}

	/**
	 * Create an {@link AdminClient}; invoke the callback and reliably close the admin.
	 * @param callback the callback.
	 * @param <T> the function return type.
	 * @return a map of results.
	 * @since 2.5.4
	 */
	public <T> T doWithAdminFunction(Function<AdminClient, T> callback) {
		Map<String, Object> adminConfigs = new HashMap<>();
		adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokersAsString());
		try (AdminClient admin = AdminClient.create(adminConfigs)) {
			return callback.apply(admin);
		}
	}

	@Override
	public void destroy() {
		System.getProperties().remove(brokerListProperty);
		System.getProperties().remove(SPRING_EMBEDDED_ZOOKEEPER_CONNECT);
		for (KafkaServer kafkaServer : this.kafkaServers) {
			try {
				if (brokerRunning(kafkaServer)) {
					kafkaServer.shutdown();
					kafkaServer.awaitShutdown();
				}
			}
			catch (Exception e) {
				// do nothing
			}
			try {
				CoreUtils.delete(kafkaServer.config().logDirs());
			}
			catch (Exception e) {
				// do nothing
			}
		}
		synchronized (this) {
			if (this.zooKeeperClient != null) {
				this.zooKeeperClient.close();
			}
		}
		try {
			this.zookeeper.shutdown();
			this.zkConnect = null;
		}
		catch (Exception e) {
			// do nothing
		}
	}

	private boolean brokerRunning(KafkaServer kafkaServer) {
		try {
			return !kafkaServer.brokerState().equals(BrokerState.NOT_RUNNING);
		}
		catch (NoSuchMethodError error) {
			if (GET_BROKER_STATE_METHOD != null) {
				try {
					return !GET_BROKER_STATE_METHOD.invoke(kafkaServer).toString().equals("NOT_RUNNING");
				}
				catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
					logger.debug(ex, "Could not determine broker state during shutdown");
					return true;
				}
			}
			else {
				logger.debug("Could not determine broker state during shutdown");
				return true;
			}
		}
	}

	public Set<String> getTopics() {
		return new HashSet<>(this.topics);
	}

	public List<KafkaServer> getKafkaServers() {
		return this.kafkaServers;
	}

	public KafkaServer getKafkaServer(int id) {
		return this.kafkaServers.get(id);
	}

	public EmbeddedZookeeper getZookeeper() {
		return this.zookeeper;
	}

	/**
	 * Return the ZooKeeperClient.
	 * @return the client.
	 * @since 2.3.2
	 */
	public synchronized ZooKeeperClient getZooKeeperClient() {
		if (this.zooKeeperClient == null) {
			this.zooKeeperClient = new ZooKeeperClient(this.zkConnect, zkSessionTimeout, zkConnectionTimeout,
					1, Time.SYSTEM, "embeddedKafkaZK", "embeddedKafkaZK", new ZKClientConfig(), "embeddedKafkaZK");
		}
		return this.zooKeeperClient;
	}

	public String getZookeeperConnectionString() {
		return this.zkConnect;
	}

	public BrokerAddress getBrokerAddress(int i) {
		KafkaServer kafkaServer = this.kafkaServers.get(i);
		return new BrokerAddress(LOOPBACK, kafkaServer.config().listeners().apply(0).port());
	}

	public BrokerAddress[] getBrokerAddresses() {
		List<BrokerAddress> addresses = new ArrayList<BrokerAddress>();
		for (int kafkaPort : this.kafkaPorts) {
			addresses.add(new BrokerAddress(LOOPBACK, kafkaPort));
		}
		return addresses.toArray(new BrokerAddress[0]);
	}

	public int getPartitionsPerTopic() {
		return this.partitionsPerTopic;
	}

	public void bounce(BrokerAddress brokerAddress) {
		for (KafkaServer kafkaServer : getKafkaServers()) {
			EndPoint endpoint = kafkaServer.config().listeners().apply(0);
			if (brokerAddress.equals(new BrokerAddress(endpoint.host(), endpoint.port()))) {
				kafkaServer.shutdown();
				kafkaServer.awaitShutdown();
			}
		}
	}

	public void restart(final int index) throws Exception { //NOSONAR

		// retry restarting repeatedly, first attempts may fail

		SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(10, // NOSONAR magic #
				Collections.singletonMap(Exception.class, true));

		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(100); // NOSONAR magic #
		backOffPolicy.setMaxInterval(1000); // NOSONAR magic #
		backOffPolicy.setMultiplier(2); // NOSONAR magic #

		RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(retryPolicy);
		retryTemplate.setBackOffPolicy(backOffPolicy);


		retryTemplate.execute(context -> {
			this.kafkaServers.get(index).startup();
			return null;
		});
	}

	public String getBrokersAsString() {
		StringBuilder builder = new StringBuilder();
		for (BrokerAddress brokerAddress : getBrokerAddresses()) {
			builder.append(brokerAddress.toString()).append(',');
		}
		return builder.substring(0, builder.length() - 1);
	}

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param consumer the consumer.
	 */
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer) {
		consumeFromEmbeddedTopics(consumer, this.topics.toArray(new String[0]));
	}

	/**
	 * Subscribe a consumer to all the embedded topics.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @param consumer the consumer.
	 * @since 2.8.2
	 */
	public void consumeFromAllEmbeddedTopics(Consumer<?, ?> consumer, boolean seekToEnd) {
		consumeFromEmbeddedTopics(consumer, seekToEnd, this.topics.toArray(new String[0]));
	}

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param topic the topic.
	 */
	public void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, String topic) {
		consumeFromEmbeddedTopics(consumer, topic);
	}

	/**
	 * Subscribe a consumer to one of the embedded topics.
	 * @param consumer the consumer.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @param topic the topic.
	 * @since 2.8.2
	 */
	public void consumeFromAnEmbeddedTopic(Consumer<?, ?> consumer, boolean seekToEnd, String topic) {
		consumeFromEmbeddedTopics(consumer, seekToEnd, topic);
	}

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topicsToConsume the topics.
	 * @throws IllegalStateException if you attempt to consume from a topic that is not in
	 * the list of embedded topics (since 2.3.4).
	 */
	public void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, String... topicsToConsume) {
		consumeFromEmbeddedTopics(consumer, false, topicsToConsume);
	}

	/**
	 * Subscribe a consumer to one or more of the embedded topics.
	 * @param consumer the consumer.
	 * @param topicsToConsume the topics.
	 * @param seekToEnd true to seek to the end instead of the beginning.
	 * @throws IllegalStateException if you attempt to consume from a topic that is not in
	 * the list of embedded topics.
	 * @since 2.8.2
	 */
	public void consumeFromEmbeddedTopics(Consumer<?, ?> consumer, boolean seekToEnd, String... topicsToConsume) {
		List<String> notEmbedded = Arrays.stream(topicsToConsume)
				.filter(topic -> !this.topics.contains(topic))
				.collect(Collectors.toList());
		if (notEmbedded.size() > 0) {
			throw new IllegalStateException("topic(s):'" + notEmbedded + "' are not in embedded topic list");
		}
		final AtomicReference<Collection<TopicPartition>> assigned = new AtomicReference<>();
		consumer.subscribe(Arrays.asList(topicsToConsume), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				assigned.set(partitions);
				logger.debug(() -> "partitions assigned: " + partitions);
			}

		});
		int n = 0;
		while (assigned.get() == null && n++ < 600) { // NOSONAR magic #
			consumer.poll(Duration.ofMillis(100)); // force assignment NOSONAR magic #
		}
		if (assigned.get() != null) {
			logger.debug(() -> "Partitions assigned "
					+ assigned.get()
					+ "; re-seeking to "
					+ (seekToEnd ? "end; " : "beginning"));
			if (seekToEnd) {
				consumer.seekToEnd(assigned.get());
			}
			else {
				consumer.seekToBeginning(assigned.get());
			}
		}
		else {
			throw new IllegalStateException("Failed to be assigned partitions from the embedded topics");
		}
		logger.debug("Subscription Initiated");
	}

	/**
	 * Ported from scala to allow setting the port.
	 *
	 * @author Gary Russell
	 * @since 2.3
	 */
	public static final class EmbeddedZookeeper {

		private static final int THREE_K = 3000;

		private static final int HUNDRED = 100;

		private static final int TICK_TIME = 800; // allow a maxSessionTimeout of 20 * 800ms = 16 secs

		private final NIOServerCnxnFactory factory;

		private final ZooKeeperServer zookeeper;

		private final int port;

		private final File snapshotDir;

		private final File logDir;

		public EmbeddedZookeeper(int zkPort) throws IOException, InterruptedException {
			this.snapshotDir = TestUtils.tempDir();
			this.logDir = TestUtils.tempDir();
			System.setProperty("zookeeper.forceSync", "no"); // disable fsync to ZK txn
																// log in tests to avoid
																// timeout
			this.zookeeper = new ZooKeeperServer(this.snapshotDir, this.logDir, TICK_TIME);
			this.factory = new NIOServerCnxnFactory();
			InetSocketAddress addr = new InetSocketAddress(LOOPBACK, zkPort == 0 ? TestUtils.RandomPort() : zkPort);
			this.factory.configure(addr, 0);
			this.factory.startup(zookeeper);
			this.port = zookeeper.getClientPort();
		}

		public int getPort() {
			return this.port;
		}

		public File getSnapshotDir() {
			return this.snapshotDir;
		}

		public File getLogDir() {
			return this.logDir;
		}

		public void shutdown() throws IOException {
			// Also shuts down ZooKeeperServer
			try {
				this.factory.shutdown();
			}
			catch (Exception e) {
				logger.error(e, "ZK shutdown failed");
			}

			int n = 0;
			while (n++ < HUNDRED) {
				try {
					ZkFourLetterWords.sendStat(LOOPBACK, this.port, THREE_K);
					Thread.sleep(HUNDRED);
				}
				catch (@SuppressWarnings("unused") Exception e) {
					break;
				}
			}
			if (n == HUNDRED) {
				logger.debug("Zookeeper failed to stop");
			}

			try {
				this.zookeeper.getZKDatabase().close();
			}
			catch (Exception e) {
				logger.error(e, "ZK db close failed");
			}

			Utils.delete(this.logDir);
			Utils.delete(this.snapshotDir);
		}

	}

}
