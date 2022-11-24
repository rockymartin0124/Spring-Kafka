/*
 * Copyright 2014-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.adapter.BatchToRecordAdapter;
import org.springframework.kafka.listener.adapter.FilteringBatchMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.listener.adapter.ReplyHeadersConfigurer;
import org.springframework.kafka.support.JavaUtils;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Base model for a Kafka listener endpoint.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @see MethodKafkaListenerEndpoint
 */
public abstract class AbstractKafkaListenerEndpoint<K, V>
		implements KafkaListenerEndpoint, BeanFactoryAware, InitializingBean {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private String id;

	private String groupId;

	private final Collection<String> topics = new ArrayList<>();

	private Pattern topicPattern;

	private final Collection<TopicPartitionOffset> topicPartitions = new ArrayList<>();

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	private BeanResolver beanResolver;

	private String group;

	private RecordFilterStrategy<K, V> recordFilterStrategy;

	private boolean ackDiscarded;

	private Boolean batchListener;

	private KafkaTemplate<?, ?> replyTemplate;

	private String clientIdPrefix;

	private Integer concurrency;

	private Boolean autoStartup;

	private ReplyHeadersConfigurer replyHeadersConfigurer;

	private Properties consumerProperties;

	private boolean splitIterables = true;

	private BatchToRecordAdapter<K, V> batchToRecordAdapter;

	private byte[] listenerInfo;

	private String correlationHeaderName;

	@Nullable
	private String mainListenerId;

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory) {
			this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
		}
		this.beanResolver = new BeanFactoryResolver(beanFactory);
	}

	@Nullable
	protected BeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	@Nullable
	protected BeanExpressionResolver getResolver() {
		return this.resolver;
	}

	@Nullable
	protected BeanExpressionContext getBeanExpressionContext() {
		return this.expressionContext;
	}

	@Nullable
	protected BeanResolver getBeanResolver() {
		return this.beanResolver;
	}

	public void setId(@Nullable String id) {
		this.id = id;
	}

	public void setMainListenerId(@Nullable String id) {
		this.mainListenerId = id;
	}

	@Override
	@Nullable
	public String getMainListenerId() {
		return this.mainListenerId;
	}

	@Nullable
	@Override
	public String getId() {
		return this.id;
	}

	/**
	 * Set the group id to override the {@code group.id} property in the
	 * ContainerFactory.
	 * @param groupId the group id.
	 * @since 1.3
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	@Nullable
	@Override
	public String getGroupId() {
		return this.groupId;
	}

	/**
	 * Set the topics to use. Either these or 'topicPattern' or 'topicPartitions'
	 * should be provided, but not a mixture.
	 * @param topics to set.
	 * @see #setTopicPartitions(TopicPartitionOffset...)
	 * @see #setTopicPattern(Pattern)
	 */
	public void setTopics(String... topics) {
		Assert.notNull(topics, "'topics' must not be null");
		this.topics.clear();
		this.topics.addAll(Arrays.asList(topics));
	}

	/**
	 * Return the topics for this endpoint.
	 * @return the topics for this endpoint.
	 */
	@Override
	public Collection<String> getTopics() {
		return Collections.unmodifiableCollection(this.topics);
	}

	/**
	 * Set the topicPartitions to use.
	 * Either this or 'topic' or 'topicPattern'
	 * should be provided, but not a mixture.
	 * @param topicPartitions to set.
	 * @since 2.3
	 * @see #setTopics(String...)
	 * @see #setTopicPattern(Pattern)
	 */
	public void setTopicPartitions(TopicPartitionOffset... topicPartitions) {
		Assert.notNull(topicPartitions, "'topics' must not be null");
		this.topicPartitions.clear();
		this.topicPartitions.addAll(Arrays.asList(topicPartitions));
	}

	/**
	 * Return the topicPartitions for this endpoint.
	 * @return the topicPartitions for this endpoint.
	 * @since 2.3
	 */
	@Nullable
	@Override
	public TopicPartitionOffset[] getTopicPartitionsToAssign() {
		return this.topicPartitions.toArray(new TopicPartitionOffset[0]);
	}

	/**
	 * Set the topic pattern to use. Cannot be used with
	 * topics or topicPartitions.
	 * @param topicPattern the pattern
	 * @see #setTopicPartitions(TopicPartitionOffset...)
	 * @see #setTopics(String...)
	 */
	public void setTopicPattern(Pattern topicPattern) {
		this.topicPattern = topicPattern;
	}

	/**
	 * Return the topicPattern for this endpoint.
	 * @return the topicPattern for this endpoint.
	 */
	@Nullable
	@Override
	public Pattern getTopicPattern() {
		return this.topicPattern;
	}

	@Nullable
	@Override
	public String getGroup() {
		return this.group;
	}

	/**
	 * Set the group for the corresponding listener container.
	 * @param group the group.
	 */
	public void setGroup(String group) {
		this.group = group;
	}

	/**
	 * Return true if this endpoint creates a batch listener.
	 * @return true for a batch listener.
	 * @since 1.1
	 */
	public boolean isBatchListener() {
		return this.batchListener == null ? false : this.batchListener;
	}

	/**
	 * Return the current batch listener flag for this endpoint, or null if not explicitly
	 * set.
	 * @return the batch listener flag.
	 * @since 2.8
	 */
	@Nullable
	public Boolean getBatchListener() {
		return this.batchListener;
	}

	/**
	 * Set to true if this endpoint should create a batch listener.
	 * @param batchListener true for a batch listener.
	 * @since 1.1
	 */
	public void setBatchListener(boolean batchListener) {
		this.batchListener = batchListener;
	}

	/**
	 * Set the {@link KafkaTemplate} to use to send replies.
	 * @param replyTemplate the template.
	 * @since 2.0
	 */
	public void setReplyTemplate(KafkaTemplate<?, ?> replyTemplate) {
		this.replyTemplate = replyTemplate;
	}

	@Nullable
	protected KafkaTemplate<?, ?> getReplyTemplate() {
		return this.replyTemplate;
	}

	@Nullable
	protected RecordFilterStrategy<? super K, ? super V> getRecordFilterStrategy() {
		return this.recordFilterStrategy;
	}

	/**
	 * Set a {@link RecordFilterStrategy} implementation.
	 * @param recordFilterStrategy the strategy implementation.
	 */
	@SuppressWarnings("unchecked")
	public void setRecordFilterStrategy(RecordFilterStrategy<? super K, ? super V> recordFilterStrategy) {
		this.recordFilterStrategy = (RecordFilterStrategy<K, V>) recordFilterStrategy;
	}

	protected boolean isAckDiscarded() {
		return this.ackDiscarded;
	}

	/**
	 * Set to true if the {@link #setRecordFilterStrategy(RecordFilterStrategy)} is in use.
	 * @param ackDiscarded the ackDiscarded.
	 */
	public void setAckDiscarded(boolean ackDiscarded) {
		this.ackDiscarded = ackDiscarded;
	}

	@Nullable
	@Override
	public String getClientIdPrefix() {
		return this.clientIdPrefix;
	}

	/**
	 * Set the client id prefix; overrides the client id in the consumer configuration
	 * properties.
	 * @param clientIdPrefix the prefix.
	 * @since 2.1.1
	 */
	public void setClientIdPrefix(String clientIdPrefix) {
		this.clientIdPrefix = clientIdPrefix;
	}

	@Override
	@Nullable
	public Integer getConcurrency() {
		return this.concurrency;
	}

	/**
	 * Set the concurrency for this endpoint's container.
	 * @param concurrency the concurrency.
	 * @since 2.2
	 */
	public void setConcurrency(Integer concurrency) {
		this.concurrency = concurrency;
	}

	@Override
	@Nullable
	public Boolean getAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Set the autoStartup for this endpoint's container.
	 * @param autoStartup the autoStartup.
	 * @since 2.2
	 */
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * Set a configurer which will be invoked when creating a reply message.
	 * @param replyHeadersConfigurer the configurer.
	 * @since 2.2
	 */
	public void setReplyHeadersConfigurer(ReplyHeadersConfigurer replyHeadersConfigurer) {
		this.replyHeadersConfigurer = replyHeadersConfigurer;
	}

	@Override
	@Nullable
	public Properties getConsumerProperties() {
		return this.consumerProperties;
	}

	/**
	 * Set the consumer properties that will be merged with the consumer properties
	 * provided by the consumer factory; properties here will supersede any with the same
	 * name(s) in the consumer factory.
	 * {@code group.id} and {@code client.id} are ignored.
	 * @param consumerProperties the properties.
	 * @since 2.1.4
	 * @see org.apache.kafka.clients.consumer.ConsumerConfig
	 * @see #setGroupId(String)
	 * @see #setClientIdPrefix(String)
	 */
	public void setConsumerProperties(Properties consumerProperties) {
		this.consumerProperties = consumerProperties;
	}

	@Override
	public boolean isSplitIterables() {
		return this.splitIterables;
	}

	/**
	 * Set to false to disable splitting {@link Iterable} reply values into separate
	 * records.
	 * @param splitIterables false to disable; default true.
	 * @since 2.3.5
	 */
	public void setSplitIterables(boolean splitIterables) {
		this.splitIterables = splitIterables;
	}

	@Override
	@Nullable
	public byte[] getListenerInfo() {
		return this.listenerInfo; // NOSONAR
	}

	/**
	 * Set the listener info to insert in the record header.
	 * @param listenerInfo the info.
	 * @since 2.8.4
	 */
	public void setListenerInfo(@Nullable byte[] listenerInfo) {  // NOSONAR
		this.listenerInfo = listenerInfo; // NOSONAR
	}

	@Nullable
	protected BatchToRecordAdapter<K, V> getBatchToRecordAdapter() {
		return this.batchToRecordAdapter;
	}

	/**
	 * Set a {@link BatchToRecordAdapter}.
	 * @param batchToRecordAdapter the adapter.
	 * @since 2.4.2
	 */
	public void setBatchToRecordAdapter(BatchToRecordAdapter<K, V> batchToRecordAdapter) {
		this.batchToRecordAdapter = batchToRecordAdapter;
	}

	/**
	 * Set a custom header name for the correlation id. Default
	 * {@link org.springframework.kafka.support.KafkaHeaders#CORRELATION_ID}. This header
	 * will be echoed back in any reply message.
	 * @param correlationHeaderName the header name.
	 * @since 3.0
	 */
	public void setCorrelationHeaderName(String correlationHeaderName) {
		this.correlationHeaderName = correlationHeaderName;
	}

	@Override
	public void afterPropertiesSet() {
		boolean topicsEmpty = getTopics().isEmpty();
		boolean topicPartitionsEmpty = ObjectUtils.isEmpty(getTopicPartitionsToAssign());
		if (!topicsEmpty && !topicPartitionsEmpty) {
			throw new IllegalStateException("Topics or topicPartitions must be provided but not both for " + this);
		}
		if (this.topicPattern != null && (!topicsEmpty || !topicPartitionsEmpty)) {
			throw new IllegalStateException("Only one of topics, topicPartitions or topicPattern must are allowed for "
					+ this);
		}
		if (this.topicPattern == null && topicsEmpty && topicPartitionsEmpty) {
			throw new IllegalStateException("At least one of topics, topicPartitions or topicPattern must be provided "
					+ "for " + this);
		}
	}

	@Override
	public void setupListenerContainer(MessageListenerContainer listenerContainer,
			@Nullable MessageConverter messageConverter) {

		setupMessageListener(listenerContainer, messageConverter);
	}

	/**
	 * Create a {@link MessageListener} that is able to serve this endpoint for the
	 * specified container.
	 * @param container the {@link MessageListenerContainer} to create a {@link MessageListener}.
	 * @param messageConverter the message converter - may be null.
	 * @return a {@link MessageListener} instance.
	 */
	protected abstract MessagingMessageListenerAdapter<K, V> createMessageListener(MessageListenerContainer container,
			@Nullable MessageConverter messageConverter);

	@SuppressWarnings("unchecked")
	private void setupMessageListener(MessageListenerContainer container,
			@Nullable MessageConverter messageConverter) {

		MessagingMessageListenerAdapter<K, V> adapter = createMessageListener(container, messageConverter);
		JavaUtils.INSTANCE
				.acceptIfNotNull(this.replyHeadersConfigurer, adapter::setReplyHeadersConfigurer)
				.acceptIfNotNull(this.correlationHeaderName, adapter::setCorrelationHeaderName);
		adapter.setSplitIterables(this.splitIterables);
		Object messageListener = adapter;
		boolean isBatchListener = isBatchListener();
		Assert.state(messageListener != null,
				() -> "Endpoint [" + this + "] must provide a non null message listener");
		if (this.recordFilterStrategy != null) {
			if (isBatchListener) {
				if (((MessagingMessageListenerAdapter<K, V>) messageListener).isConsumerRecords()) {
					this.logger.warn(() -> "Filter strategy ignored when consuming 'ConsumerRecords' instead of a List"
							+ (this.id != null ? " id: " + this.id : ""));
				}
				else {
					messageListener = new FilteringBatchMessageListenerAdapter<>(
							(BatchMessageListener<K, V>) messageListener, this.recordFilterStrategy, this.ackDiscarded);
				}
			}
			else {
				messageListener = new FilteringMessageListenerAdapter<>((MessageListener<K, V>) messageListener,
						this.recordFilterStrategy, this.ackDiscarded);
			}
		}
		container.setupMessageListener(messageListener);
	}

	/**
	 * Return a description for this endpoint.
	 * @return a description for this endpoint.
	 * <p>Available to subclasses, for inclusion in their {@code toString()} result.
	 */
	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append(getClass().getSimpleName()).append("[").append(this.id).
				append("] topics=").append(this.topics).
				append("' | topicPartitions='").append(this.topicPartitions).
				append("' | topicPattern='").append(this.topicPattern).append("'");
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

}
