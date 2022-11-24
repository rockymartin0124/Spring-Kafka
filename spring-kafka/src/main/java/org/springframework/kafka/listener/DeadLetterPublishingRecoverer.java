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

package org.springframework.kafka.listener;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * A {@link ConsumerRecordRecoverer} that publishes a failed record to a dead-letter
 * topic.
 *
 * @author Gary Russell
 * @author Tomaz Fernandes
 * @since 2.2
 *
 */
public class DeadLetterPublishingRecoverer extends ExceptionClassifier implements ConsumerAwareRecordRecoverer {

	private static final BiFunction<ConsumerRecord<?, ?>, Exception, Headers> DEFAULT_HEADERS_FUNCTION =
			(rec, ex) -> null;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private static final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>
		DEFAULT_DESTINATION_RESOLVER = (cr, e) -> new TopicPartition(cr.topic() + ".DLT", cr.partition());

	private static final long FIVE = 5L;

	private static final long THIRTY = 30L;

	private final HeaderNames headerNames = getHeaderNames();

	private final boolean transactional;

	private final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver;

	private final Function<ProducerRecord<?, ?>, KafkaOperations<?, ?>> templateResolver;

	private final EnumSet<HeaderNames.HeadersToAdd> whichHeaders = EnumSet.allOf(HeaderNames.HeadersToAdd.class);

	private boolean retainExceptionHeader;

	private BiFunction<ConsumerRecord<?, ?>, Exception, Headers> headersFunction = DEFAULT_HEADERS_FUNCTION;

	private boolean verifyPartition = true;

	private Duration partitionInfoTimeout = Duration.ofSeconds(FIVE);

	private Duration waitForSendResultTimeout = Duration.ofSeconds(THIRTY);

	private boolean appendOriginalHeaders = true;

	private boolean failIfSendResultIsError = true;

	private boolean throwIfNoDestinationReturned = false;

	private long timeoutBuffer = Duration.ofSeconds(FIVE).toMillis();

	private boolean stripPreviousExceptionHeaders = true;

	private boolean skipSameTopicFatalExceptions = true;

	private ExceptionHeadersCreator exceptionHeadersCreator = this::addExceptionInfoHeaders;

	/**
	 * Create an instance with the provided template and a default destination resolving
	 * function that returns a TopicPartition based on the original topic (appended with ".DLT")
	 * from the failed record, and the same partition as the failed record. Therefore the
	 * dead-letter topic must have at least as many partitions as the original topic.
	 * @param template the {@link KafkaOperations} to use for publishing.
	 */
	public DeadLetterPublishingRecoverer(KafkaOperations<? extends Object, ? extends Object> template) {
		this(template, DEFAULT_DESTINATION_RESOLVER);
	}

	/**
	 * Create an instance with the provided template and destination resolving function,
	 * that receives the failed consumer record and the exception and returns a
	 * {@link TopicPartition}. If the partition in the {@link TopicPartition} is less than
	 * 0, no partition is set when publishing to the topic.
	 * @param template the {@link KafkaOperations} to use for publishing.
	 * @param destinationResolver the resolving function.
	 */
	public DeadLetterPublishingRecoverer(KafkaOperations<? extends Object, ? extends Object> template,
			BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
		this(Collections.singletonMap(Object.class, template), destinationResolver);
	}

	/**
	 * Create an instance with the provided templates and a default destination resolving
	 * function that returns a TopicPartition based on the original topic (appended with
	 * ".DLT") from the failed record, and the same partition as the failed record.
	 * Therefore the dead-letter topic must have at least as many partitions as the
	 * original topic. The templates map keys are classes and the value the corresponding
	 * template to use for objects (producer record values) of that type. A
	 * {@link java.util.LinkedHashMap} is recommended when there is more than one
	 * template, to ensure the map is traversed in order. To send records with a null
	 * value, add a template with the {@link Void} class as a key; otherwise the first
	 * template from the map values iterator will be used.
	 * @param templates the {@link KafkaOperations}s to use for publishing.
	 */
	public DeadLetterPublishingRecoverer(Map<Class<?>, KafkaOperations<? extends Object, ? extends Object>> templates) {
		this(templates, DEFAULT_DESTINATION_RESOLVER);
	}

	/**
	 * Create an instance with the provided templates and destination resolving function,
	 * that receives the failed consumer record and the exception and returns a
	 * {@link TopicPartition}. If the partition in the {@link TopicPartition} is less than
	 * 0, no partition is set when publishing to the topic. The templates map keys are
	 * classes and the value the corresponding template to use for objects (producer
	 * record values) of that type. A {@link java.util.LinkedHashMap} is recommended when
	 * there is more than one template, to ensure the map is traversed in order. To send
	 * records with a null value, add a template with the {@link Void} class as a key;
	 * otherwise the first template from the map values iterator will be used.
	 * @param templates the {@link KafkaOperations}s to use for publishing.
	 * @param destinationResolver the resolving function.
	 */
	@SuppressWarnings("unchecked")
	public DeadLetterPublishingRecoverer(Map<Class<?>, KafkaOperations<? extends Object, ? extends Object>> templates,
			BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {

		Assert.isTrue(!ObjectUtils.isEmpty(templates), "At least one template is required");
		Assert.notNull(destinationResolver, "The destinationResolver cannot be null");
		KafkaOperations<?, ?> firstTemplate = templates.values().iterator().next();
		this.templateResolver = templates.size() == 1
				? producerRecord -> firstTemplate
				: producerRecord -> findTemplateForValue(producerRecord.value(), templates);
		this.transactional = firstTemplate.isTransactional();
		Boolean tx = this.transactional;
		Assert.isTrue(templates.values()
			.stream()
			.map(t -> t.isTransactional())
			.allMatch(t -> t.equals(tx)), "All templates must have the same setting for transactional");
		this.destinationResolver = destinationResolver;
	}

	/**
	* Create an instance with a template resolving function that receives the failed
	* consumer record and the exception and returns a {@link KafkaOperations} and a
	* flag on whether or not the publishing from this instance will be transactional
	* or not. Also receives a destination resolving function that works similarly but
	* returns a {@link TopicPartition} instead. If the partition in the {@link TopicPartition}
	* is less than 0, no partition is set when publishing to the topic.
	*
	* @param templateResolver the function that resolver the {@link KafkaOperations} to use for publishing.
	* @param transactional whether or not publishing by this instance should be transactional
	* @param destinationResolver the resolving function.
	* @since 2.7
	*/
	public DeadLetterPublishingRecoverer(Function<ProducerRecord<?, ?>, KafkaOperations<?, ?>> templateResolver,
										boolean transactional,
										BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {

		Assert.notNull(templateResolver, "The templateResolver cannot be null");
		Assert.notNull(destinationResolver, "The destinationResolver cannot be null");
		this.transactional = transactional;
		this.destinationResolver = destinationResolver;
		this.templateResolver = templateResolver;
	}

	/**
	 * Set to true to retain a Java serialized {@link DeserializationException} header. By
	 * default, such headers are removed from the published record, unless both key and
	 * value deserialization exceptions occur, in which case, the DLT_* headers are
	 * created from the value exception and the key exception header is retained.
	 * @param retainExceptionHeader true to retain the
	 * @since 2.5
	 */
	public void setRetainExceptionHeader(boolean retainExceptionHeader) {
		this.retainExceptionHeader = retainExceptionHeader;
	}

	/**
	 * Set a function which will be called to obtain additional headers to add to the
	 * published record.
	 * @param headersFunction the headers function.
	 * @since 2.5.4
	 * @see #addHeadersFunction(BiFunction)
	 */
	public void setHeadersFunction(BiFunction<ConsumerRecord<?, ?>, Exception, Headers> headersFunction) {
		Assert.notNull(headersFunction, "'headersFunction' cannot be null");
		if (!this.headersFunction.equals(DEFAULT_HEADERS_FUNCTION)) {
			this.logger.warn(() -> "Replacing custom headers function: " + this.headersFunction
					+ ", consider using addHeadersFunction() if you need multiple functions");
		}
		this.headersFunction = headersFunction;
	}

	/**
	 * Set to false to disable partition verification. When true, verify that the
	 * partition returned by the resolver actually exists. If not, set the
	 * {@link ProducerRecord#partition()} to null, allowing the producer to determine the
	 * destination partition.
	 * @param verifyPartition false to disable.
	 * @since 2.7
	 * @see #setPartitionInfoTimeout(Duration)
	 */
	public void setVerifyPartition(boolean verifyPartition) {
		this.verifyPartition = verifyPartition;
	}

	/**
	 * Time to wait for partition information when verifying. Default is 5 seconds.
	 * @param partitionInfoTimeout the timeout.
	 * @since 2.7
	 * @see #setVerifyPartition(boolean)
	 */
	public void setPartitionInfoTimeout(Duration partitionInfoTimeout) {
		Assert.notNull(partitionInfoTimeout, "'partitionInfoTimeout' cannot be null");
		this.partitionInfoTimeout = partitionInfoTimeout;
	}

	/**
	 * Set to false if you don't want to append the current "original" headers (topic,
	 * partition etc.) if they are already present. When false, only the first "original"
	 * headers are retained.
	 * @param appendOriginalHeaders set to false not to replace.
	 * @since 2.7.9
	 */
	public void setAppendOriginalHeaders(boolean appendOriginalHeaders) {
		this.appendOriginalHeaders = appendOriginalHeaders;
	}

	/**
	 * Set to true to throw an exception if the destination resolver function returns
	 * a null TopicPartition.
	 * @param throwIfNoDestinationReturned true to enable.
	 * @since 2.7
	 */
	public void setThrowIfNoDestinationReturned(boolean throwIfNoDestinationReturned) {
		this.throwIfNoDestinationReturned = throwIfNoDestinationReturned;
	}

	/**
	 * Set to true to enable waiting for the send result and throw an exception if it fails.
	 * It will wait for the milliseconds specified in waitForSendResultTimeout for the result.
	 * @param failIfSendResultIsError true to enable.
	 * @since 2.7
	 * @see #setWaitForSendResultTimeout(Duration)
	 */
	public void setFailIfSendResultIsError(boolean failIfSendResultIsError) {
		this.failIfSendResultIsError = failIfSendResultIsError;
	}

	/**
	 * If true, wait for the send result and throw an exception if it fails.
	 * It will wait for the milliseconds specified in waitForSendResultTimeout for the result.
	 * @return true to wait.
	 * @since 2.7.14
	 * @see #setWaitForSendResultTimeout(Duration)
	 */
	protected boolean isFailIfSendResultIsError() {
		return this.failIfSendResultIsError;
	}

	/**
	 * Set the minimum time to wait for message sending. Default is the producer
	 * configuration {@code delivery.timeout.ms} plus the {@link #setTimeoutBuffer(long)}.
	 * @param waitForSendResultTimeout the timeout.
	 * @since 2.7
	 * @see #setFailIfSendResultIsError(boolean)
	 * @see #setTimeoutBuffer(long)
	 */
	public void setWaitForSendResultTimeout(Duration waitForSendResultTimeout) {
		this.waitForSendResultTimeout = waitForSendResultTimeout;
	}

	/**
	 * Set the number of milliseconds to add to the producer configuration
	 * {@code delivery.timeout.ms} property to avoid timing out before the Kafka producer.
	 * Default 5000.
	 * @param buffer the buffer.
	 * @since 2.7
	 * @see #setWaitForSendResultTimeout(Duration)
	 */
	public void setTimeoutBuffer(long buffer) {
		this.timeoutBuffer = buffer;
	}

	/**
	 * The number of milliseconds to add to the producer configuration
	 * {@code delivery.timeout.ms} property to avoid timing out before the Kafka producer.
	 * @return the buffer.
	 * @since 2.7.14
	 */
	protected long getTimeoutBuffer() {
		return this.timeoutBuffer;
	}

	/**
	 * Set to false to retain previous exception headers as well as headers for the
	 * current exception. Default is true, which means only the current headers are
	 * retained; setting it to false this can cause a growth in record size when a record
	 * is republished many times.
	 * @param stripPreviousExceptionHeaders false to retain all.
	 * @since 2.7.9
	 */
	public void setStripPreviousExceptionHeaders(boolean stripPreviousExceptionHeaders) {
		this.stripPreviousExceptionHeaders = stripPreviousExceptionHeaders;
	}

	/**
	 * Set to false if you want to forward the record to the same topic even though
	 * the exception is fatal by this class' classification, e.g. to handle this scenario
	 * in a different layer.
	 * @param skipSameTopicFatalExceptions false to forward in this scenario.
	 */
	public void setSkipSameTopicFatalExceptions(boolean skipSameTopicFatalExceptions) {
		this.skipSameTopicFatalExceptions = skipSameTopicFatalExceptions;
	}

	/**
	 * Set a {@link ExceptionHeadersCreator} implementation to completely take over
	 * setting the exception headers in the output record. Disables all headers that are
	 * set by default.
	 * @param headersCreator the creator.
	 * @since 2.8.4
	 */
	public void setExceptionHeadersCreator(ExceptionHeadersCreator headersCreator) {
		Assert.notNull(headersCreator, "'headersCreator' cannot be null");
		this.exceptionHeadersCreator = headersCreator;
	}

	/**
	 * True if publishing should run in a transaction.
	 * @return true for transactional.
	 * @since 2.7.14
	 */
	protected boolean isTransactional() {
		return this.transactional;
	}

	/**
	 * Clear the header inclusion bit for the header name.
	 * @param headers the headers to clear.
	 * @since 2.8.4
	 */
	public void excludeHeader(HeaderNames.HeadersToAdd... headers) {
		Assert.notNull(headers, "'headers' cannot be null");
		Assert.noNullElements(headers, "'headers' cannot include null elements");
		for (HeaderNames.HeadersToAdd header : headers) {
			this.whichHeaders.remove(header);
		}
	}

	/**
	 * Set the header inclusion bit for the header name.
	 * @param headers the headers to set.
	 * @since 2.8.4
	 */
	public void includeHeader(HeaderNames.HeadersToAdd... headers) {
		Assert.notNull(headers, "'headers' cannot be null");
		Assert.noNullElements(headers, "'headers' cannot include null elements");
		for (HeaderNames.HeadersToAdd header : headers) {
			this.whichHeaders.add(header);
		}
	}

	/**
	 * Add a function which will be called to obtain additional headers to add to the
	 * published record. Functions are called in the order that they are added, and after
	 * any function passed into {@link #setHeadersFunction(BiFunction)}.
	 * @param headersFunction the headers function.
	 * @since 2.8.4
	 * @see #setHeadersFunction(BiFunction)
	 */
	public void addHeadersFunction(BiFunction<ConsumerRecord<?, ?>, Exception, Headers> headersFunction) {
		Assert.notNull(headersFunction, "'headersFunction' cannot be null");
		if (this.headersFunction.equals(DEFAULT_HEADERS_FUNCTION)) {
			this.headersFunction = headersFunction;
		}
		else {
			BiFunction<ConsumerRecord<?, ?>, Exception, Headers> toCompose = this.headersFunction;
			this.headersFunction = (rec, ex) -> {
				Headers headers1 = toCompose.apply(rec, ex);
				if (headers1 == null) {
					headers1 = new RecordHeaders();
				}
				Headers headers2 = headersFunction.apply(rec, ex);
				try {
					if (headers2 != null) {
						headers2.forEach(headers1::add);
					}
				}
				catch (IllegalStateException isex) {
					headers1 = new RecordHeaders(headers1);
					headers2.forEach(headers1::add); // NOSONAR, never null here
				}
				return headers1;
			};
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void accept(ConsumerRecord<?, ?> record, @Nullable Consumer<?, ?> consumer, Exception exception) {
		TopicPartition tp = this.destinationResolver.apply(record, exception);
		if (tp == null) {
			maybeThrow(record, exception);
			this.logger.debug(() -> "Recovery of " + KafkaUtils.format(record)
					+ " skipped because destination resolver returned null");
			return;
		}
		if (this.skipSameTopicFatalExceptions
				&& tp.topic().equals(record.topic())
				&& !getClassifier().classify(exception)) {
			this.logger.error("Recovery of " + KafkaUtils.format(record)
					+ " skipped because not retryable exception " + exception.toString()
					+ " and the destination resolver routed back to the same topic");
			return;
		}
		if (consumer != null && this.verifyPartition) {
			tp = checkPartition(tp, consumer);
		}
		DeserializationException vDeserEx = ListenerUtils.getExceptionFromHeader(record,
				SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, this.logger);
		DeserializationException kDeserEx = ListenerUtils.getExceptionFromHeader(record,
				SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, this.logger);
		Headers headers = new RecordHeaders(record.headers().toArray());
		addAndEnhanceHeaders(record, exception, vDeserEx, kDeserEx, headers);
		ProducerRecord<Object, Object> outRecord = createProducerRecord(record, tp, headers,
				kDeserEx == null ? null : kDeserEx.getData(), vDeserEx == null ? null : vDeserEx.getData());
		KafkaOperations<Object, Object> kafkaTemplate =
				(KafkaOperations<Object, Object>) this.templateResolver.apply(outRecord);
		sendOrThrow(outRecord, kafkaTemplate, record);
	}

	private void addAndEnhanceHeaders(ConsumerRecord<?, ?> record, Exception exception,
			@Nullable DeserializationException vDeserEx, @Nullable DeserializationException kDeserEx, Headers headers) {

		if (kDeserEx != null) {
			if (!this.retainExceptionHeader) {
				headers.remove(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER);
			}
			this.exceptionHeadersCreator.create(headers, kDeserEx, true, this.headerNames);
		}
		if (vDeserEx != null) {
			if (!this.retainExceptionHeader) {
				headers.remove(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
			}
			this.exceptionHeadersCreator.create(headers, vDeserEx, false, this.headerNames);
		}
		if (kDeserEx == null && vDeserEx == null) {
			this.exceptionHeadersCreator.create(headers, exception, false, this.headerNames);
		}
		enhanceHeaders(headers, record, exception); // NOSONAR headers are never null
	}

	private void sendOrThrow(ProducerRecord<Object, Object> outRecord,
			@Nullable KafkaOperations<Object, Object> kafkaTemplate, ConsumerRecord<?, ?> inRecord) {

		if (kafkaTemplate != null) {
			send(outRecord, kafkaTemplate, inRecord);
		}
		else {
			throw new IllegalArgumentException("No kafka template returned for record " + outRecord);
		}
	}

	private void maybeThrow(ConsumerRecord<?, ?> record, Exception exception) {
		String message = String.format("No destination returned for record %s and exception %s. " +
				"failIfNoDestinationReturned: %s", KafkaUtils.format(record), exception,
				this.throwIfNoDestinationReturned);
		this.logger.warn(message);
		if (this.throwIfNoDestinationReturned) {
			throw new IllegalArgumentException(message);
		}
	}

	/**
	 * Send the record.
	 * @param outRecord the record.
	 * @param kafkaTemplate the template.
	 * @param inRecord the consumer record.
	 * @since 2.7
	 */
	protected void send(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> kafkaTemplate,
			ConsumerRecord<?, ?> inRecord) {

		if (this.transactional && !kafkaTemplate.inTransaction() && !kafkaTemplate.isAllowNonTransactional()) {
			kafkaTemplate.executeInTransaction(t -> {
				publish(outRecord, t, inRecord);
				return null;
			});
		}
		else {
			publish(outRecord, kafkaTemplate, inRecord);
		}
	}

	private TopicPartition checkPartition(TopicPartition tp, Consumer<?, ?> consumer) {
		if (tp.partition() < 0) {
			return tp;
		}
		try {
			List<PartitionInfo> partitions = consumer.partitionsFor(tp.topic(), this.partitionInfoTimeout);
			if (partitions == null) {
				this.logger.debug(() -> "Could not obtain partition info for " + tp.topic());
				return tp;
			}
			boolean anyMatch = partitions.stream().anyMatch(pi -> pi.partition() == tp.partition());
			if (!anyMatch) {
				this.logger.warn(() -> "Destination resolver returned non-existent partition " + tp
						+ ", KafkaProducer will determine partition to use for this topic");
				return new TopicPartition(tp.topic(), -1);
			}
			return tp;
		}
		catch (Exception ex) {
			this.logger.debug(ex, () -> "Could not obtain partition info for " + tp.topic());
			return tp;
		}
	}

	@SuppressWarnings("unchecked")
	private KafkaOperations<Object, Object> findTemplateForValue(@Nullable Object value,
			Map<Class<?>, KafkaOperations<?, ?>> templates) {

		if (value == null) {
			KafkaOperations<?, ?> operations = templates.get(Void.class);
			if (operations == null) {
				return (KafkaOperations<Object, Object>) templates.values().iterator().next();
			}
			else {
				return (KafkaOperations<Object, Object>) operations;
			}
		}
		Optional<Class<?>> key = templates.keySet()
			.stream()
			.filter((k) -> k.isAssignableFrom(value.getClass()))
			.findFirst();
		if (key.isPresent()) {
			return (KafkaOperations<Object, Object>) templates.get(key.get());
		}
		this.logger.warn(() -> "Failed to find a template for " + value.getClass() + " attempting to use the last entry");
		return (KafkaOperations<Object, Object>) templates.values()
				.stream()
				.reduce((first,  second) -> second)
				.get();
	}

	/**
	 * Subclasses can override this method to customize the producer record to send to the
	 * DLQ. The default implementation simply copies the key and value from the consumer
	 * record and adds the headers. The timestamp is not set (the original timestamp is in
	 * one of the headers). IMPORTANT: if the partition in the {@link TopicPartition} is
	 * less than 0, it must be set to null in the {@link ProducerRecord}.
	 * @param record the failed record
	 * @param topicPartition the {@link TopicPartition} returned by the destination
	 * resolver.
	 * @param headers the headers - original record headers plus DLT headers.
	 * @param key the key to use instead of the consumer record key.
	 * @param value the value to use instead of the consumer record value.
	 * @return the producer record to send.
	 * @see KafkaHeaders
	 */
	protected ProducerRecord<Object, Object> createProducerRecord(ConsumerRecord<?, ?> record,
			TopicPartition topicPartition, Headers headers, @Nullable byte[] key, @Nullable byte[] value) {

		return new ProducerRecord<>(topicPartition.topic(),
				topicPartition.partition() < 0 ? null : topicPartition.partition(),
				key != null ? key : record.key(),
				value != null ? value : record.value(), headers);
	}

	/**
	 * Override this if you want more than just logging of the send result.
	 * @param outRecord the record to send.
	 * @param kafkaTemplate the template.
	 * @param inRecord the consumer record.
	 * @since 2.2.5
	 */
	protected void publish(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> kafkaTemplate,
			ConsumerRecord<?, ?> inRecord) {

		CompletableFuture<SendResult<Object, Object>> sendResult = null;
		try {
			sendResult = kafkaTemplate.send(outRecord);
			sendResult.whenComplete((result, ex) -> {
				if (ex == null) {
					this.logger.debug(() -> "Successful dead-letter publication: "
							+ KafkaUtils.format(inRecord) + " to " + result.getRecordMetadata());
				}
				else {
					this.logger.error(ex, () -> pubFailMessage(outRecord, inRecord));
				}
			});
		}
		catch (Exception e) {
			this.logger.error(e, () -> pubFailMessage(outRecord, inRecord));
		}
		if (this.failIfSendResultIsError) {
			verifySendResult(kafkaTemplate, outRecord, sendResult, inRecord);
		}
	}

	/**
	 * Wait for the send future to complete.
	 * @param kafkaTemplate the template used to send the record.
	 * @param outRecord the record.
	 * @param sendResult the future.
	 * @param inRecord the original consumer record.
	 */
	protected void verifySendResult(KafkaOperations<Object, Object> kafkaTemplate,
			ProducerRecord<Object, Object> outRecord,
			@Nullable CompletableFuture<SendResult<Object, Object>> sendResult, ConsumerRecord<?, ?> inRecord) {

		Duration sendTimeout = determineSendTimeout(kafkaTemplate);
		if (sendResult == null) {
			throw new KafkaException(pubFailMessage(outRecord, inRecord));
		}
		try {
			sendResult.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new KafkaException(pubFailMessage(outRecord, inRecord), e);
		}
		catch (ExecutionException | TimeoutException e) {
			throw new KafkaException(pubFailMessage(outRecord, inRecord), e);
		}
	}

	private String pubFailMessage(ProducerRecord<Object, Object> outRecord, ConsumerRecord<?, ?> inRecord) {
		return "Dead-letter publication to "
				+ outRecord.topic() + " failed for: " + KafkaUtils.format(inRecord);
	}

	/**
	 * Determine the send timeout based on the template's producer factory and
	 * {@link #setWaitForSendResultTimeout(Duration)}.
	 * @param template the template.
	 * @return the timeout.
	 * @since 2.7.14
	 */
	protected Duration determineSendTimeout(KafkaOperations<?, ?> template) {
		ProducerFactory<? extends Object, ? extends Object> producerFactory = template.getProducerFactory();
		if (producerFactory != null) { // NOSONAR - will only occur in mock tests
			Map<String, Object> props;
			try {
				props = producerFactory.getConfigurationProperties();
			}
			catch (UnsupportedOperationException ex) {
				props = Collections.emptyMap();
			}
			if (props != null) { // NOSONAR - will only occur in mock tests
				return KafkaUtils.determineSendTimeout(props, this.timeoutBuffer,
						this.waitForSendResultTimeout.toMillis());
			}
		}
		return Duration.ofSeconds(THIRTY);
	}

	private void enhanceHeaders(Headers kafkaHeaders, ConsumerRecord<?, ?> record, Exception exception) {
		maybeAddOriginalHeaders(kafkaHeaders, record, exception);
		Headers headers = this.headersFunction.apply(record, exception);
		if (headers != null) {
			headers.forEach(kafkaHeaders::add);
		}
	}

	private void maybeAddOriginalHeaders(Headers kafkaHeaders, ConsumerRecord<?, ?> record, Exception ex) {
		maybeAddHeader(kafkaHeaders, this.headerNames.original.topicHeader,
				() -> record.topic().getBytes(StandardCharsets.UTF_8), HeaderNames.HeadersToAdd.TOPIC);
		maybeAddHeader(kafkaHeaders, this.headerNames.original.partitionHeader,
				() -> ByteBuffer.allocate(Integer.BYTES).putInt(record.partition()).array(),
				HeaderNames.HeadersToAdd.PARTITION);
		maybeAddHeader(kafkaHeaders, this.headerNames.original.offsetHeader,
				() -> ByteBuffer.allocate(Long.BYTES).putLong(record.offset()).array(),
				HeaderNames.HeadersToAdd.OFFSET);
		maybeAddHeader(kafkaHeaders, this.headerNames.original.timestampHeader,
				() -> ByteBuffer.allocate(Long.BYTES).putLong(record.timestamp()).array(), HeaderNames.HeadersToAdd.TS);
		maybeAddHeader(kafkaHeaders, this.headerNames.original.timestampTypeHeader,
				() -> record.timestampType().toString().getBytes(StandardCharsets.UTF_8),
				HeaderNames.HeadersToAdd.TS_TYPE);
		if (ex instanceof ListenerExecutionFailedException) {
			String consumerGroup = ((ListenerExecutionFailedException) ex).getGroupId();
			if (consumerGroup != null) {
				maybeAddHeader(kafkaHeaders, this.headerNames.original.consumerGroup,
						() -> consumerGroup.getBytes(StandardCharsets.UTF_8), HeaderNames.HeadersToAdd.GROUP);
			}
		}
	}

	private void maybeAddHeader(Headers kafkaHeaders, String header, Supplier<byte[]> valueSupplier,
			HeaderNames.HeadersToAdd hta) {

		if (this.whichHeaders.contains(hta)
				&& (this.appendOriginalHeaders || kafkaHeaders.lastHeader(header) == null)) {
			kafkaHeaders.add(header, valueSupplier.get());
		}
	}

	private void addExceptionInfoHeaders(Headers kafkaHeaders, Exception exception, boolean isKey,
			HeaderNames names) {

		appendOrReplace(kafkaHeaders,
				isKey ? names.exceptionInfo.keyExceptionFqcn : names.exceptionInfo.exceptionFqcn,
				() -> exception.getClass().getName().getBytes(StandardCharsets.UTF_8),
				HeaderNames.HeadersToAdd.EXCEPTION);
		if (exception.getCause() != null) {
			appendOrReplace(kafkaHeaders,
					names.exceptionInfo.exceptionCauseFqcn,
					() -> exception.getCause().getClass().getName().getBytes(StandardCharsets.UTF_8),
					HeaderNames.HeadersToAdd.EX_CAUSE);
		}
		String message = exception.getMessage();
		if (message != null) {
			appendOrReplace(kafkaHeaders,
					isKey ? names.exceptionInfo.keyExceptionMessage : names.exceptionInfo.exceptionMessage,
					() -> exception.getMessage().getBytes(StandardCharsets.UTF_8),
					HeaderNames.HeadersToAdd.EX_MSG);
		}
		appendOrReplace(kafkaHeaders,
				isKey ? names.exceptionInfo.keyExceptionStacktrace : names.exceptionInfo.exceptionStacktrace,
				() -> getStackTraceAsString(exception).getBytes(StandardCharsets.UTF_8),
				HeaderNames.HeadersToAdd.EX_STACKTRACE);
	}

	private void appendOrReplace(Headers headers, String header, Supplier<byte[]> valueSupplier,
			HeaderNames.HeadersToAdd hta) {

		if (this.whichHeaders.contains(hta)) {
			if (this.stripPreviousExceptionHeaders) {
				headers.remove(header);
			}
			headers.add(header, valueSupplier.get());
		}
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

	/**
	 * Override this if you want different header names to be used
	 * in the sent record.
	 * @return the header names.
	 * @since 2.7
	 */
	protected HeaderNames getHeaderNames() {
		return HeaderNames.Builder
				.original()
					.offsetHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET)
					.timestampHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP)
					.timestampTypeHeader(KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE)
					.topicHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC)
					.partitionHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION)
					.consumerGroupHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)
				.exception()
					.keyExceptionFqcn(KafkaHeaders.DLT_KEY_EXCEPTION_FQCN)
					.exceptionFqcn(KafkaHeaders.DLT_EXCEPTION_FQCN)
					.exceptionCauseFqcn(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
					.keyExceptionMessage(KafkaHeaders.DLT_KEY_EXCEPTION_MESSAGE)
					.exceptionMessage(KafkaHeaders.DLT_EXCEPTION_MESSAGE)
					.keyExceptionStacktrace(KafkaHeaders.DLT_KEY_EXCEPTION_STACKTRACE)
					.exceptionStacktrace(KafkaHeaders.DLT_EXCEPTION_STACKTRACE)
				.build();
	}

	/**
	 * Container class for the name of the headers that will
	 * be added to the produced record.
	 * @since 2.7
	 */
	public static class HeaderNames {

		/**
		 * Bits representing which headers to add.
		 * @since 2.8.4
		 */
		public enum HeadersToAdd {

			/**
			 * The offset of the failed record.
			 */
			OFFSET,

			/**
			 * The timestamp of the failed record.
			 */
			TS,

			/**
			 * The timestamp type of the failed record.
			 */
			TS_TYPE,

			/**
			 * The original topic of the failed record.
			 */
			TOPIC,

			/**
			 * The partition from which the failed record was received.
			 */
			PARTITION,

			/**
			 * The consumer group that received the failed record.
			 */
			GROUP,

			/**
			 * The exception class name.
			 */
			EXCEPTION,

			/**
			 * The exception cause class name.
			 */
			EX_CAUSE,

			/**
			 * The exception message.
			 */
			EX_MSG,

			/**
			 * The exception stack trace.
			 */
			EX_STACKTRACE;

		}

		private final HeaderNames.Original original;

		private final ExceptionInfo exceptionInfo;

		HeaderNames(HeaderNames.Original original, ExceptionInfo exceptionInfo) {
			this.original = original;
			this.exceptionInfo = exceptionInfo;
		}

		/**
		 * The header names for the original record headers.
		 * @return the original.
		 * @since 2.8.4
		 */
		public HeaderNames.Original getOriginal() {
			return this.original;
		}

		/**
		 * The header names for the exception headers.
		 * @return the exceptionInfo
		 * @since 2.8.4
		 */
		public ExceptionInfo getExceptionInfo() {
			return this.exceptionInfo;
		}

		/**
		 * Header names for original record property headers.
		 *
		 * @since 2.8.4
		 */
		public static class Original {

			final String offsetHeader; // NOSONAR

			final String timestampHeader; // NOSONAR

			final String timestampTypeHeader; // NOSONAR

			final String topicHeader; // NOSONAR

			final String partitionHeader; // NOSONAR

			final String consumerGroup; // NOSONAR

			Original(String offsetHeader,
					String timestampHeader,
					String timestampTypeHeader,
					String topicHeader,
					String partitionHeader,
					String consumerGroup) {
				this.offsetHeader = offsetHeader;
				this.timestampHeader = timestampHeader;
				this.timestampTypeHeader = timestampTypeHeader;
				this.topicHeader = topicHeader;
				this.partitionHeader = partitionHeader;
				this.consumerGroup = consumerGroup;
			}

			/**
			 * The header name for the offset.
			 * @return the offsetHeader.
			 */
			public String getOffsetHeader() {
				return this.offsetHeader;
			}

			/**
			 * The header name for the timestamp.
			 * @return the timestampHeader.
			 */
			public String getTimestampHeader() {
				return this.timestampHeader;
			}

			/**
			 * The header name for the timestamp type.
			 * @return the timestampTypeHeader.
			 */
			public String getTimestampTypeHeader() {
				return this.timestampTypeHeader;
			}

			/**
			 * The header name for the topic.
			 * @return the topicHeader.
			 */
			public String getTopicHeader() {
				return this.topicHeader;
			}

			/**
			 * The header name for the partition.
			 * @return the partitionHeader
			 */
			public String getPartitionHeader() {
				return this.partitionHeader;
			}

			/**
			 * The header name for the consumer group.
			 * @return the consumerGroup
			 */
			public String getConsumerGroup() {
				return this.consumerGroup;
			}

		}

		/**
		 * Header names for exception headers.
		 *
		 * @since 2.8.4
		 */
		public static class ExceptionInfo {

			final String keyExceptionFqcn; // NOSONAR

			final String exceptionFqcn; // NOSONAR

			final String exceptionCauseFqcn; // NOSONAR

			final String keyExceptionMessage; // NOSONAR

			final String exceptionMessage; // NOSONAR

			final String keyExceptionStacktrace; // NOSONAR

			final String exceptionStacktrace; // NOSONAR

			ExceptionInfo(String keyExceptionFqcn,
					String exceptionFqcn,
					String exceptionCauseFqcn,
					String keyExceptionMessage,
					String exceptionMessage,
					String keyExceptionStacktrace,
					String exceptionStacktrace) {
				this.keyExceptionFqcn = keyExceptionFqcn;
				this.exceptionFqcn = exceptionFqcn;
				this.exceptionCauseFqcn = exceptionCauseFqcn;
				this.keyExceptionMessage = keyExceptionMessage;
				this.exceptionMessage = exceptionMessage;
				this.keyExceptionStacktrace = keyExceptionStacktrace;
				this.exceptionStacktrace = exceptionStacktrace;
			}

			/**
			 * The header name for the key exception class.
			 * @return the keyExceptionFqcn.
			 */
			public String getKeyExceptionFqcn() {
				return this.keyExceptionFqcn;
			}

			/**
			 * The header name for the value exception class.
			 * @return the exceptionFqcn.
			 */
			public String getExceptionFqcn() {
				return this.exceptionFqcn;
			}

			/**
			 * The header name for the exception cause.
			 * @return the exceptionCauseFqcn.
			 */
			public String getExceptionCauseFqcn() {
				return this.exceptionCauseFqcn;
			}

			/**
			 * The header name for the key exception message.
			 * @return the keyExceptionMessage.
			 */
			public String getKeyExceptionMessage() {
				return this.keyExceptionMessage;
			}

			/**
			 * The header name for the exception message.
			 * @return the exceptionMessage.
			 */
			public String getExceptionMessage() {
				return this.exceptionMessage;
			}

			/**
			 * The header name for the key exception stack trace.
			 * @return the keyExceptionStacktrace
			 */
			public String getKeyExceptionStacktrace() {
				return this.keyExceptionStacktrace;
			}

			/**
			 * The header name for the exception stack trace.
			 * @return the exceptionStacktrace
			 */
			public String getExceptionStacktrace() {
				return this.exceptionStacktrace;
			}

		}

		/**
		 * Provides a convenient API for creating
		 * {@link DeadLetterPublishingRecoverer.HeaderNames}.
		 *
		 * @author Tomaz Fernandes
		 * @since 2.7
		 * @see HeaderNames
		 */
		public static class Builder {

			private final Original original = new Original();

			private final ExceptionInfo exceptionInfo = new ExceptionInfo();

			public static Builder.Original original() {
				return new Builder().original;
			}

			/**
			 * Headers for data relative to the original record.
			 *
			 * @author Tomaz Fernandes
			 * @since 2.7
			 */
			public class Original {

				private String offsetHeader;

				private String timestampHeader;

				private String timestampTypeHeader;

				private String topicHeader;

				private String partitionHeader;

				private String consumerGroupHeader;

				/**
				 * Sets the name of the header that will be used to store the offset
				 * of the original record.
				 * @param offsetHeader the offset header name.
				 * @return the Original builder instance
				 * @since 2.7
				 */
				public Builder.Original offsetHeader(String offsetHeader) {
					this.offsetHeader = offsetHeader;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the timestamp
				 * of the original record.
				 * @param timestampHeader the timestamp header name.
				 * @return the Original builder instance
				 * @since 2.7
				 */
				public Builder.Original timestampHeader(String timestampHeader) {
					this.timestampHeader = timestampHeader;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the timestampType
				 * of the original record.
				 * @param timestampTypeHeader the timestampType header name.
				 * @return the Original builder instance
				 * @since 2.7
				 */
				public Builder.Original timestampTypeHeader(String timestampTypeHeader) {
					this.timestampTypeHeader = timestampTypeHeader;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the topic
				 * of the original record.
				 * @param topicHeader the topic header name.
				 * @return the Original builder instance
				 * @since 2.7
				 */
				public Builder.Original topicHeader(String topicHeader) {
					this.topicHeader = topicHeader;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the partition
				 * of the original record.
				 * @param partitionHeader the partition header name.
				 * @return the Original builder instance
				 * @since 2.7
				 */
				public Builder.Original partitionHeader(String partitionHeader) {
					this.partitionHeader = partitionHeader;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the consumer
				 * group that failed to consume the original record.
				 * @param consumerGroupHeader the consumer group header name.
				 * @return the Original builder instance
				 * @since 2.8
				 */
				public Builder.Original consumerGroupHeader(String consumerGroupHeader) {
					this.consumerGroupHeader = consumerGroupHeader;
					return this;
				}

				/**
				 * Returns the exception builder.
				 * @return the exception builder.
				 * @since 2.7
				 */
				public ExceptionInfo exception() {
					return Builder.this.exceptionInfo;
				}

				/**
				 * Builds the Original header names, asserting that none of them is null.
				 * @return the Original header names.
				 * @since 2.7
				 */
				private DeadLetterPublishingRecoverer.HeaderNames.Original build() {
					Assert.notNull(this.offsetHeader, "offsetHeader cannot be null");
					Assert.notNull(this.timestampHeader, "timestampHeader cannot be null");
					Assert.notNull(this.timestampTypeHeader, "timestampTypeHeader cannot be null");
					Assert.notNull(this.topicHeader, "topicHeader cannot be null");
					Assert.notNull(this.partitionHeader, "partitionHeader cannot be null");
					Assert.notNull(this.consumerGroupHeader, "consumerGroupHeader cannot be null");
					return new DeadLetterPublishingRecoverer.HeaderNames.Original(this.offsetHeader,
							this.timestampHeader,
							this.timestampTypeHeader,
							this.topicHeader,
							this.partitionHeader,
							this.consumerGroupHeader);
				}
			}

			/**
			 * Headers for data relative to the exception thrown.
			 *
			 * @author Tomaz Fernandes
			 * @since 2.7
			 */
			public class ExceptionInfo {

				private String keyExceptionFqcn;

				private String exceptionFqcn;

				private String exceptionCauseFqcn;

				private String keyExceptionMessage;

				private String exceptionMessage;

				private String keyExceptionStacktrace;

				private String exceptionStacktrace;

				/**
				 * Sets the name of the header that will be used to store the keyExceptionFqcn
				 * of the original record.
				 * @param keyExceptionFqcn the keyExceptionFqcn header name.
				 * @return the Exception builder instance
				 * @since 2.7
				 */
				public ExceptionInfo keyExceptionFqcn(String keyExceptionFqcn) {
					this.keyExceptionFqcn = keyExceptionFqcn;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the exceptionFqcn
				 * of the original record.
				 * @param exceptionFqcn the exceptionFqcn header name.
				 * @return the Exception builder instance
				 * @since 2.7
				 */
				public ExceptionInfo exceptionFqcn(String exceptionFqcn) {
					this.exceptionFqcn = exceptionFqcn;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the exceptionCauseFqcn
				 * of the original record.
				 * @param exceptionCauseFqcn the exceptionFqcn header name.
				 * @return the Exception builder instance
				 * @since 2.8
				 */
				public ExceptionInfo exceptionCauseFqcn(String exceptionCauseFqcn) {
					this.exceptionCauseFqcn = exceptionCauseFqcn;
					return this;
				}
				/**
				 * Sets the name of the header that will be used to store the keyExceptionMessage
				 * of the original record.
				 * @param keyExceptionMessage the keyExceptionMessage header name.
				 * @return the Exception builder instance
				 * @since 2.7
				 */
				public ExceptionInfo keyExceptionMessage(String keyExceptionMessage) {
					this.keyExceptionMessage = keyExceptionMessage;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the exceptionMessage
				 * of the original record.
				 * @param exceptionMessage the exceptionMessage header name.
				 * @return the Exception builder instance
				 * @since 2.7
				 */
				public ExceptionInfo exceptionMessage(String exceptionMessage) {
					this.exceptionMessage = exceptionMessage;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the
				 * keyExceptionStacktrace of the original record.
				 * @param keyExceptionStacktrace the keyExceptionStacktrace header name.
				 * @return the Exception builder instance
				 * @since 2.7
				 */
				public ExceptionInfo keyExceptionStacktrace(String keyExceptionStacktrace) {
					this.keyExceptionStacktrace = keyExceptionStacktrace;
					return this;
				}

				/**
				 * Sets the name of the header that will be used to store the
				 * exceptionStacktrace of the original record.
				 * @param exceptionStacktrace the exceptionStacktrace header name.
				 * @return the Exception builder instance
				 * @since 2.7
				 */
				public ExceptionInfo exceptionStacktrace(String exceptionStacktrace) {
					this.exceptionStacktrace = exceptionStacktrace;
					return this;
				}

				/**
				 * Builds the Header Names, asserting that none of them is null.
				 * @return the HeaderNames instance.
				 * @since 2.7
				 */
				public DeadLetterPublishingRecoverer.HeaderNames build() {
					Assert.notNull(this.keyExceptionFqcn, "keyExceptionFqcn header cannot be null");
					Assert.notNull(this.exceptionFqcn, "exceptionFqcn header cannot be null");
					Assert.notNull(this.exceptionCauseFqcn, "exceptionCauseFqcn header cannot be null");
					Assert.notNull(this.keyExceptionMessage, "keyExceptionMessage header cannot be null");
					Assert.notNull(this.exceptionMessage, "exceptionMessage header cannot be null");
					Assert.notNull(this.keyExceptionStacktrace, "keyExceptionStacktrace header cannot be null");
					Assert.notNull(this.exceptionStacktrace, "exceptionStacktrace header cannot be null");
					return new DeadLetterPublishingRecoverer.HeaderNames(Builder.this.original.build(),
							new HeaderNames.ExceptionInfo(this.keyExceptionFqcn,
									this.exceptionFqcn,
									this.exceptionCauseFqcn,
									this.keyExceptionMessage,
									this.exceptionMessage,
									this.keyExceptionStacktrace,
									this.exceptionStacktrace));
				}
			}
		}

	}

	/**
	 * Use this to provide a custom implementation to take complete control over exception
	 * header creation for the output record.
	 *
	 * @since 2.8.4
	 */
	public interface ExceptionHeadersCreator {

		/**
		 * Create exception headers.
		 * @param kafkaHeaders the {@link Headers} to add the header(s) to.
		 * @param exception The exception.
		 * @param isKey whether the exception is for a key or value.
		 * @param headerNames the heaader names to use.
		 */
		void create(Headers kafkaHeaders, Exception exception, boolean isKey, HeaderNames headerNames);

	}

}
