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

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.expression.MapAccessor;
import org.springframework.core.MethodParameter;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.expression.spel.support.StandardTypeConverter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * An abstract {@link org.springframework.kafka.listener.MessageListener} adapter
 * providing the necessary infrastructure to extract the payload of a
 * {@link org.springframework.messaging.Message}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Venil Noronha
 */
public abstract class MessagingMessageListenerAdapter<K, V> implements ConsumerSeekAware {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	/**
	 * Message used when no conversion is needed.
	 */
	protected static final Message<KafkaNull> NULL_MESSAGE = new GenericMessage<>(KafkaNull.INSTANCE); // NOSONAR

	private final Object bean;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); //NOSONAR

	private final Type inferredType;

	private final StandardEvaluationContext evaluationContext = new StandardEvaluationContext();

	private HandlerAdapter handlerMethod;

	private boolean isConsumerRecordList;

	private boolean isConsumerRecords;

	private boolean isMessageList;

	private boolean conversionNeeded = true;

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private boolean converterSet;

	private Type fallbackType = Object.class;

	private Expression replyTopicExpression;

	@SuppressWarnings("rawtypes")
	private KafkaTemplate replyTemplate;

	private boolean hasAckParameter;

	private boolean hasMetadataParameter;

	private boolean messageReturnType;

	private ReplyHeadersConfigurer replyHeadersConfigurer;

	private boolean splitIterables = true;

	private String correlationHeaderName = KafkaHeaders.CORRELATION_ID;

	/**
	 * Create an instance with the provided bean and method.
	 * @param bean the bean.
	 * @param method the method.
	 */
	protected MessagingMessageListenerAdapter(Object bean, Method method) {
		this.bean = bean;
		this.inferredType = determineInferredType(method); // NOSONAR = intentionally not final
	}

	/**
	 * Set a custom header name for the correlation id. Default
	 * {@link KafkaHeaders#CORRELATION_ID}. This header will be echoed back in any reply
	 * message.
	 * @param correlationHeaderName the header name.
	 * @since 3.0
	 */
	public void setCorrelationHeaderName(String correlationHeaderName) {
		Assert.notNull(correlationHeaderName, "'correlationHeaderName' cannot be null");
		this.correlationHeaderName = correlationHeaderName;
	}

	/**
	 * Set the MessageConverter.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(RecordMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
		this.converterSet = true;
	}

	/**
	 * Return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final RecordMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Set the {@link SmartMessageConverter} to use with the default
	 * {@link MessagingMessageConverter}. Not allowed when a custom
	 * {@link #setMessageConverter(RecordMessageConverter) messageConverter} is provided.
	 * @param messageConverter the converter.
	 * @since 2.7.1
	 */
	public void setMessagingConverter(SmartMessageConverter messageConverter) {
		Assert.isTrue(!this.converterSet, "Cannot set the SmartMessageConverter when setting the messageConverter, "
				+ "add the SmartConverter to the message converter instead");
		((MessagingMessageConverter) this.messageConverter).setMessagingConverter(messageConverter);
	}

	/**
	 * Returns the inferred type for conversion or, if null, the
	 * {@link #setFallbackType(Class) fallbackType}.
	 * @return the type.
	 */
	protected Type getType() {
		return this.inferredType == null ? this.fallbackType : this.inferredType;
	}

	/**
	 * Set a fallback type to use when using a type-aware message converter and this
	 * adapter cannot determine the inferred type from the method. An example of a
	 * type-aware message converter is the {@code StringJsonMessageConverter}. Defaults to
	 * {@link Object}.
	 * @param fallbackType the type.
	 */
	public void setFallbackType(Class<?> fallbackType) {
		this.fallbackType = fallbackType;
	}

	/**
	 * Set the {@link HandlerAdapter} to use to invoke the method
	 * processing an incoming {@link ConsumerRecord}.
	 * @param handlerMethod {@link HandlerAdapter} instance.
	 */
	public void setHandlerMethod(HandlerAdapter handlerMethod) {
		this.handlerMethod = handlerMethod;
	}

	protected boolean isConsumerRecordList() {
		return this.isConsumerRecordList;
	}

	public boolean isConsumerRecords() {
		return this.isConsumerRecords;
	}

	public boolean isConversionNeeded() {
		return this.conversionNeeded;
	}

	/**
	 * Set the topic to which to send any result from the method invocation.
	 * May be a SpEL expression {@code !{...}} evaluated at runtime.
	 * @param replyTopicParam the topic or expression.
	 * @since 2.0
	 */
	public void setReplyTopic(String replyTopicParam) {
		String replyTopic = replyTopicParam;
		if (!StringUtils.hasText(replyTopic)) {
			replyTopic = AdapterUtils.getDefaultReplyTopicExpression();
		}
		if (replyTopic.contains(AdapterUtils.PARSER_CONTEXT.getExpressionPrefix())) {
			this.replyTopicExpression = PARSER.parseExpression(replyTopic, AdapterUtils.PARSER_CONTEXT);
		}
		else {
			this.replyTopicExpression = new LiteralExpression(replyTopic);
		}

	}

	/**
	 * Set the template to use to send any result from the method invocation.
	 * @param replyTemplate the template.
	 * @since 2.0
	 */
	public void setReplyTemplate(KafkaTemplate<?, ?> replyTemplate) {
		this.replyTemplate = replyTemplate;
	}

	/**
	 * Set a bean resolver for runtime SpEL expressions. Also configures the evaluation
	 * context with a standard type converter and map accessor.
	 * @param beanResolver the resolver.
	 * @since 2.0
	 */
	public void setBeanResolver(BeanResolver beanResolver) {
		this.evaluationContext.setBeanResolver(beanResolver);
		this.evaluationContext.setTypeConverter(new StandardTypeConverter());
		this.evaluationContext.addPropertyAccessor(new MapAccessor());
	}

	protected boolean isMessageList() {
		return this.isMessageList;
	}

	/**
	 * Return the reply configurer.
	 * @return the configurer.
	 * @since 2.2
	 * @see #setReplyHeadersConfigurer(ReplyHeadersConfigurer)
	 */
	protected ReplyHeadersConfigurer getReplyHeadersConfigurer() {
		return this.replyHeadersConfigurer;
	}

	/**
	 * Set a configurer which will be invoked when creating a reply message.
	 * @param replyHeadersConfigurer the configurer.
	 * @since 2.2
	 */
	public void setReplyHeadersConfigurer(ReplyHeadersConfigurer replyHeadersConfigurer) {
		this.replyHeadersConfigurer = replyHeadersConfigurer;
	}

	/**
	 * When true, {@link Iterable} return results will be split into discrete records.
	 * @return true to split.
	 * @since 2.3.5
	 */
	protected boolean isSplitIterables() {
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
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.registerSeekCallback(callback);
		}
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.onPartitionsAssigned(assignments, callback);
		}
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.onPartitionsRevoked(partitions);
		}
	}

	@Override
	public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware csa) {
			csa.onIdleContainer(assignments, callback);
		}
	}

	protected Message<?> toMessagingMessage(ConsumerRecord<K, V> cRecord, @Nullable Acknowledgment acknowledgment,
			Consumer<?, ?> consumer) {
		return getMessageConverter().toMessage(cRecord, acknowledgment, consumer, getType());
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 * @param data the data to process during invocation.
	 * @param acknowledgment the acknowledgment to use if any.
	 * @param message the message to process.
	 * @param consumer the consumer.
	 * @return the result of invocation.
	 */
	protected final Object invokeHandler(Object data, @Nullable Acknowledgment acknowledgment, Message<?> message,
			Consumer<?, ?> consumer) {

		try {
			if (data instanceof List && !this.isConsumerRecordList) {
				return this.handlerMethod.invoke(message, acknowledgment, consumer);
			}
			else {
				if (this.hasMetadataParameter) {
					return this.handlerMethod.invoke(message, data, acknowledgment, consumer,
							AdapterUtils.buildConsumerRecordMetadata(data));
				}
				else {
					return this.handlerMethod.invoke(message, data, acknowledgment, consumer);
				}
			}
		}
		catch (org.springframework.messaging.converter.MessageConversionException ex) {
			throw checkAckArg(acknowledgment, message, new MessageConversionException("Cannot handle message", ex));
		}
		catch (MethodArgumentNotValidException ex) {
			throw checkAckArg(acknowledgment, message, ex);
		}
		catch (MessagingException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()), ex);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
					this.handlerMethod.getMethodAsString(message.getPayload()) + "' threw exception", ex);
		}
	}

	private RuntimeException checkAckArg(@Nullable Acknowledgment acknowledgment, Message<?> message, Exception ex) {
		if (this.hasAckParameter && acknowledgment == null) {
			return new ListenerExecutionFailedException("invokeHandler Failed",
					new IllegalStateException("No Acknowledgment available as an argument, "
							+ "the listener container must have a MANUAL AckMode to populate the Acknowledgment.",
							ex));
		}
		return new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
				"be invoked with the incoming message", message.getPayload()), ex);
	}

	/**
	 * Handle the given result object returned from the listener method, sending a
	 * response message to the SendTo topic.
	 * @param resultArg the result object to handle (never <code>null</code>)
	 * @param request the original request message
	 * @param source the source data for the method invocation - e.g.
	 * {@code o.s.messaging.Message<?>}; may be null
	 */
	protected void handleResult(Object resultArg, Object request, Object source) {
		this.logger.debug(() -> "Listener method returned result [" + resultArg
				+ "] - generating response message for it");
		boolean isInvocationResult = resultArg instanceof InvocationResult;
		Object result = isInvocationResult ? ((InvocationResult) resultArg).getResult() : resultArg;
		String replyTopic = evaluateReplyTopic(request, source, resultArg);
		Assert.state(replyTopic == null || this.replyTemplate != null,
				"a KafkaTemplate is required to support replies");
		sendResponse(result, replyTopic, source, isInvocationResult
				? ((InvocationResult) resultArg).isMessageReturnType() : this.messageReturnType);
	}

	@Nullable
	private String evaluateReplyTopic(Object request, Object source, Object result) {
		String replyTo = null;
		if (result instanceof InvocationResult invResult) {
			replyTo = evaluateTopic(request, source, result, invResult.getSendTo());
		}
		else if (this.replyTopicExpression != null) {
			replyTo = evaluateTopic(request, source, result, this.replyTopicExpression);
		}
		return replyTo;
	}

	@Nullable
	private String evaluateTopic(Object request, Object source, Object result, @Nullable Expression sendTo) {
		if (sendTo instanceof LiteralExpression) {
			return sendTo.getValue(String.class);
		}
		else {
			Object value = sendTo == null ? null
					: sendTo.getValue(this.evaluationContext, new ReplyExpressionRoot(request, source, result));
			boolean isByteArray = value instanceof byte[];
			if (!(value == null || value instanceof String || isByteArray)) {
				throw new IllegalStateException(
						"replyTopic expression must evaluate to a String or byte[], it is: "
								+ value.getClass().getName());
			}
			if (isByteArray) {
				return new String((byte[]) value, StandardCharsets.UTF_8); // NOSONAR
			}
			return (String) value;
		}
	}

	/**
	 * Send the result to the topic.
	 *
	 * @param result the result.
	 * @param topic the topic.
	 * @param source the source (input).
	 * @param returnTypeMessage true if we are returning message(s).
	 * @since 2.1.3
	 */
	@SuppressWarnings("unchecked")
	protected void sendResponse(Object result, String topic, @Nullable Object source, boolean returnTypeMessage) {
		if (!returnTypeMessage && topic == null) {
			this.logger.debug(() -> "No replyTopic to handle the reply: " + result);
		}
		else if (result instanceof Message) {
			Message<?> reply = checkHeaders(result, topic, source);
			this.replyTemplate.send(reply);
		}
		else {
			if (result instanceof Iterable) {
				Iterator<?> iterator = ((Iterable<?>) result).iterator();
				boolean iterableOfMessages = false;
				if (iterator.hasNext()) {
					iterableOfMessages = iterator.next() instanceof Message;
				}
				if (iterableOfMessages || this.splitIterables) {
					((Iterable<V>) result).forEach(v -> {
						if (v instanceof Message) {
							this.replyTemplate.send((Message<?>) v);
						}
						else {
							this.replyTemplate.send(topic, v);
						}
					});
				}
				else {
					sendSingleResult(result, topic, source);
				}
			}
			else {
				sendSingleResult(result, topic, source);
			}
		}
	}

	private Message<?> checkHeaders(Object result, String topic, @Nullable Object source) { // NOSONAR (complexity)
		Message<?> reply = (Message<?>) result;
		MessageHeaders headers = reply.getHeaders();
		boolean needsTopic = headers.get(KafkaHeaders.TOPIC) == null;
		boolean sourceIsMessage = source instanceof Message;
		boolean needsCorrelation = headers.get(this.correlationHeaderName) == null && sourceIsMessage;
		boolean needsPartition = headers.get(KafkaHeaders.PARTITION) == null && sourceIsMessage
				&& getReplyPartition((Message<?>) source) != null;
		if (needsTopic || needsCorrelation || needsPartition) {
			MessageBuilder<?> builder = MessageBuilder.fromMessage(reply);
			if (needsTopic) {
				builder.setHeader(KafkaHeaders.TOPIC, topic);
			}
			if (needsCorrelation && sourceIsMessage) {
				builder.setHeader(this.correlationHeaderName,
						((Message<?>) source).getHeaders().get(this.correlationHeaderName));
			}
			if (sourceIsMessage && reply.getHeaders().get(KafkaHeaders.REPLY_PARTITION) == null) {
				setPartition(builder, (Message<?>) source);
			}
			reply = builder.build();
		}
		return reply;
	}

	@SuppressWarnings("unchecked")
	private void sendSingleResult(Object result, String topic, @Nullable Object source) {
		byte[] correlationId = null;
		boolean sourceIsMessage = source instanceof Message;
		if (sourceIsMessage
				&& ((Message<?>) source).getHeaders().get(this.correlationHeaderName) != null) {
			correlationId = ((Message<?>) source).getHeaders().get(this.correlationHeaderName, byte[].class);
		}
		if (sourceIsMessage) {
			sendReplyForMessageSource(result, topic, source, correlationId);
		}
		else {
			this.replyTemplate.send(topic, result);
		}
	}

	@SuppressWarnings("unchecked")
	private void sendReplyForMessageSource(Object result, String topic, Object source, @Nullable byte[] correlationId) {
		MessageBuilder<Object> builder = MessageBuilder.withPayload(result)
				.setHeader(KafkaHeaders.TOPIC, topic);
		if (this.replyHeadersConfigurer != null) {
			Map<String, Object> headersToCopy = ((Message<?>) source).getHeaders().entrySet().stream()
					.filter(e -> {
						String key = e.getKey();
						return !key.equals(MessageHeaders.ID) && !key.equals(MessageHeaders.TIMESTAMP)
								&& !key.equals(this.correlationHeaderName)
								&& !key.startsWith(KafkaHeaders.RECEIVED);
					})
					.filter(e -> this.replyHeadersConfigurer.shouldCopy(e.getKey(), e.getValue()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			if (headersToCopy.size() > 0) {
				builder.copyHeaders(headersToCopy);
			}
			headersToCopy = this.replyHeadersConfigurer.additionalHeaders();
			if (!ObjectUtils.isEmpty(headersToCopy)) {
				builder.copyHeaders(headersToCopy);
			}
		}
		if (correlationId != null) {
			builder.setHeader(this.correlationHeaderName, correlationId);
		}
		setPartition(builder, ((Message<?>) source));
		this.replyTemplate.send(builder.build());
	}

	private void setPartition(MessageBuilder<?> builder, Message<?> source) {
		byte[] partitionBytes = getReplyPartition(source);
		if (partitionBytes != null) {
			builder.setHeader(KafkaHeaders.PARTITION, ByteBuffer.wrap(partitionBytes).getInt());
		}
	}

	@Nullable
	private byte[] getReplyPartition(Message<?> source) {
		return source.getHeaders().get(KafkaHeaders.REPLY_PARTITION, byte[].class);
	}

	protected final String createMessagingErrorMessage(String description, Object payload) {
		return description + "\n"
				+ "Endpoint handler details:\n"
				+ "Method [" + this.handlerMethod.getMethodAsString(payload) + "]\n"
				+ "Bean [" + this.handlerMethod.getBean() + "]";
	}

	/**
	 * Subclasses can override this method to use a different mechanism to determine
	 * the target type of the payload conversion.
	 * @param method the method.
	 * @return the type.
	 */
	protected Type determineInferredType(Method method) { // NOSONAR complexity
		if (method == null) {
			return null;
		}

		Type genericParameterType = null;
		int allowedBatchParameters = 1;
		int notConvertibleParameters = 0;

		for (int i = 0; i < method.getParameterCount(); i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			/*
			 * We're looking for a single non-annotated parameter, or one annotated with @Payload.
			 * We ignore parameters with type Message, Consumer, Ack, ConsumerRecord because they
			 * are not involved with conversion.
			 */
			Type parameterType = methodParameter.getGenericParameterType();
			boolean isNotConvertible = parameterIsType(parameterType, ConsumerRecord.class);
			boolean isAck = parameterIsType(parameterType, Acknowledgment.class);
			this.hasAckParameter |= isAck;
			isNotConvertible |= isAck;
			boolean isConsumer = parameterIsType(parameterType, Consumer.class);
			isNotConvertible |= isConsumer;
			boolean isMeta = parameterIsType(parameterType, ConsumerRecordMetadata.class);
			this.hasMetadataParameter |= isMeta;
			isNotConvertible |= isMeta;
			if (isNotConvertible) {
				notConvertibleParameters++;
			}
			if (!isNotConvertible && !isMessageWithNoTypeInfo(parameterType)
					&& (methodParameter.getParameterAnnotations().length == 0
					|| methodParameter.hasParameterAnnotation(Payload.class))) {
				if (genericParameterType == null) {
					genericParameterType = extractGenericParameterTypFromMethodParameter(methodParameter);
				}
				else {
					this.logger.debug(() -> "Ambiguous parameters for target payload for method " + method
							+ "; no inferred type available");
					break;
				}
			}
			else if (isAck) {
				allowedBatchParameters++;
			}
			else if (methodParameter.hasParameterAnnotation(Header.class)) {
				Header header = methodParameter.getParameterAnnotation(Header.class);
				if (header != null && KafkaHeaders.GROUP_ID.equals(header.value())) {
					allowedBatchParameters++;
				}
			}
			else {
				if (isConsumer) {
					allowedBatchParameters++;
				}
				else {
					if (parameterType instanceof ParameterizedType paramType
							&& paramType.getRawType().equals(Consumer.class)) {
						allowedBatchParameters++;
					}
				}
			}
		}

		if (notConvertibleParameters == method.getParameterCount() && method.getReturnType().equals(void.class)) {
			this.conversionNeeded = false;
		}
		boolean validParametersForBatch = method.getGenericParameterTypes().length <= allowedBatchParameters;

		if (!validParametersForBatch) {
			String stateMessage = "A parameter of type '%s' must be the only parameter "
					+ "(except for an optional 'Acknowledgment' and/or 'Consumer' "
					+ "and/or '@Header(KafkaHeaders.GROUP_ID) String groupId'";
			Assert.state(!this.isConsumerRecords,
					() -> String.format(stateMessage, "ConsumerRecords"));
			Assert.state(!this.isConsumerRecordList,
					() -> String.format(stateMessage, "List<ConsumerRecord>"));
			Assert.state(!this.isMessageList,
					() -> String.format(stateMessage, "List<Message<?>>"));
		}
		this.messageReturnType = KafkaUtils.returnTypeMessageOrCollectionOf(method);
		return genericParameterType;
	}

	private Type extractGenericParameterTypFromMethodParameter(MethodParameter methodParameter) {
		Type genericParameterType = methodParameter.getGenericParameterType();
		if (genericParameterType instanceof ParameterizedType parameterizedType) {
			if (parameterizedType.getRawType().equals(Message.class)) {
				genericParameterType = ((ParameterizedType) genericParameterType).getActualTypeArguments()[0];
			}
			else if (parameterizedType.getRawType().equals(List.class)
					&& parameterizedType.getActualTypeArguments().length == 1) {

				Type paramType = parameterizedType.getActualTypeArguments()[0];
				this.isConsumerRecordList = paramType.equals(ConsumerRecord.class)
						|| (isSimpleListOfConsumerRecord(paramType)
						|| isListOfConsumerRecordUpperBounded(paramType));
				boolean messageHasGeneric = paramType instanceof ParameterizedType pType
						&& pType.getRawType().equals(Message.class);
				this.isMessageList = paramType.equals(Message.class) || messageHasGeneric;
				if (messageHasGeneric) {
					genericParameterType = ((ParameterizedType) paramType).getActualTypeArguments()[0];
				}
			}
			else {
				this.isConsumerRecords = parameterizedType.getRawType().equals(ConsumerRecords.class);
			}
		}
		return genericParameterType;
	}

	private boolean isSimpleListOfConsumerRecord(Type paramType) {
		return paramType instanceof ParameterizedType pType && pType.getRawType().equals(ConsumerRecord.class);
	}

	private boolean isListOfConsumerRecordUpperBounded(Type paramType) {
		return isWildCardWithUpperBound(paramType)
			&& ((WildcardType) paramType).getUpperBounds()[0] instanceof ParameterizedType wildCardZero
			&& wildCardZero.getRawType().equals(ConsumerRecord.class);
	}

	private boolean isWildCardWithUpperBound(Type paramType) {
		return paramType instanceof WildcardType wcType
			&& wcType.getUpperBounds() != null
			&& wcType.getUpperBounds().length > 0;
	}

	private boolean isMessageWithNoTypeInfo(Type parameterType) {
		if (parameterType instanceof ParameterizedType parameterizedType) {
			Type rawType = parameterizedType.getRawType();
			if  (rawType.equals(Message.class)) {
				return parameterizedType.getActualTypeArguments()[0] instanceof WildcardType;
			}
		}
		return parameterType.equals(Message.class); // could be Message without a generic type
	}

	private boolean parameterIsType(Type parameterType, Type type) {
		if (parameterType instanceof ParameterizedType parameterizedType) {
			Type rawType = parameterizedType.getRawType();
			if (rawType.equals(type)) {
				return true;
			}
		}
		return parameterType.equals(type);
	}

	/**
	 * Root object for reply expression evaluation.
	 * @param request the request.
	 * @param source the source.
	 * @param result the result.
	 * @since 2.0
	 */
	public record ReplyExpressionRoot(Object request, Object source, Object result) {
	}

}
