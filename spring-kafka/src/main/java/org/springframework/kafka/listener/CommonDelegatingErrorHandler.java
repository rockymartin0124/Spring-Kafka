/*
 * Copyright 2021-2022 the original author or authors.
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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An error handler that delegates to different error handlers, depending on the exception
 * type. The delegates must have compatible properties ({@link #isAckAfterHandle()} etc.
 * {@link #deliveryAttemptHeader()} is not supported - always returns false.
 *
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @since 2.8
 *
 */
public class CommonDelegatingErrorHandler implements CommonErrorHandler {

	private final CommonErrorHandler defaultErrorHandler;

	private final Map<Class<? extends Throwable>, CommonErrorHandler> delegates = new LinkedHashMap<>();

	private boolean causeChainTraversing = false;

	private BinaryExceptionClassifier classifier = new BinaryExceptionClassifier(new HashMap<>());

	/**
	 * Construct an instance with a default error handler that will be invoked if the
	 * exception has no matches.
	 * @param defaultErrorHandler the default error handler.
	 */
	public CommonDelegatingErrorHandler(CommonErrorHandler defaultErrorHandler) {
		Assert.notNull(defaultErrorHandler, "'defaultErrorHandler' cannot be null");
		this.defaultErrorHandler = defaultErrorHandler;
	}

	/**
	 * Set the delegate error handlers; a {@link LinkedHashMap} argument is recommended so
	 * that the delegates are searched in a known order.
	 * @param delegates the delegates.
	 */
	public void setErrorHandlers(Map<Class<? extends Throwable>, CommonErrorHandler> delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		this.delegates.clear();
		this.delegates.putAll(delegates);
		checkDelegates();
		updateClassifier(delegates);
	}

	private void updateClassifier(Map<Class<? extends Throwable>, CommonErrorHandler> delegates) {
		Map<Class<? extends Throwable>, Boolean> classifications = delegates.keySet().stream()
			.map(commonErrorHandler -> Map.entry(commonErrorHandler, true))
			.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		this.classifier = new BinaryExceptionClassifier(classifications);
	}

	/**
	 * Set the flag enabling deep exception's cause chain traversing. If true, the
	 * delegate for the first exception classified by {@link BinaryExceptionClassifier}
	 * will be retrieved.
	 * @param causeChainTraversing the causeChainTraversing flag.
	 * @since 2.8.8
	 */
	public void setCauseChainTraversing(boolean causeChainTraversing) {
		this.causeChainTraversing = causeChainTraversing;
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean remainingRecords() {
		return this.defaultErrorHandler.remainingRecords();
	}

	@Override
	public boolean seeksAfterHandling() {
		return this.defaultErrorHandler.seeksAfterHandling();
	}

	@Override
	public void clearThreadState() {
		this.defaultErrorHandler.clearThreadState();
		this.delegates.values().forEach(CommonErrorHandler::clearThreadState);
	}

	@Override
	public boolean isAckAfterHandle() {
		return this.defaultErrorHandler.isAckAfterHandle();
	}

	@Override
	public void setAckAfterHandle(boolean ack) {
		this.defaultErrorHandler.setAckAfterHandle(ack);
	}

	/**
	 * Add a delegate to the end of the current collection.
	 * @param throwable the throwable for this handler.
	 * @param handler the handler.
	 */
	public void addDelegate(Class<? extends Throwable> throwable, CommonErrorHandler handler) {
		this.delegates.put(throwable, handler);
		checkDelegates();
	}

	@SuppressWarnings("deprecation")
	private void checkDelegates() {
		boolean remainingRecords = this.defaultErrorHandler.remainingRecords();
		boolean ackAfterHandle = this.defaultErrorHandler.isAckAfterHandle();
		boolean seeksAfterHandling = this.defaultErrorHandler.seeksAfterHandling();
		this.delegates.values().forEach(handler -> {
			Assert.isTrue(remainingRecords == handler.remainingRecords(),
					"All delegates must return the same value when calling 'remainingRecords()'");
			Assert.isTrue(ackAfterHandle == handler.isAckAfterHandle(),
					"All delegates must return the same value when calling 'isAckAfterHandle()'");
			Assert.isTrue(seeksAfterHandling == handler.seeksAfterHandling(),
					"All delegates must return the same value when calling 'seeksAfterHandling()'");
		});
	}

	@Override
	public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {

		CommonErrorHandler handler = findDelegate(thrownException);
		if (handler != null) {
			handler.handleRemaining(thrownException, records, consumer, container);
		}
		else {
			this.defaultErrorHandler.handleRemaining(thrownException, records, consumer, container);
		}
	}

	@Override
	public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		CommonErrorHandler handler = findDelegate(thrownException);
		if (handler != null) {
			handler.handleBatch(thrownException, data, consumer, container, invokeListener);
		}
		else {
			this.defaultErrorHandler.handleBatch(thrownException, data, consumer, container, invokeListener);
		}
	}

	@Override
	public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
			MessageListenerContainer container, boolean batchListener) {

		CommonErrorHandler handler = findDelegate(thrownException);
		if (handler != null) {
			handler.handleOtherException(thrownException, consumer, container, batchListener);
		}
		else {
			this.defaultErrorHandler.handleOtherException(thrownException, consumer, container, batchListener);
		}
	}

	@Nullable
	private CommonErrorHandler findDelegate(Throwable thrownException) {
		Throwable cause = findCause(thrownException);
		if (cause != null) {
			Class<? extends Throwable> causeClass = cause.getClass();
			for (Entry<Class<? extends Throwable>, CommonErrorHandler> entry : this.delegates.entrySet()) {
				if (entry.getKey().isAssignableFrom(causeClass)) {
					return entry.getValue();
				}
			}
		}
		return null;
	}

	@Nullable
	private Throwable findCause(Throwable thrownException) {
		if (this.causeChainTraversing) {
			return traverseCauseChain(thrownException);
		}
		return shallowTraverseCauseChain(thrownException);
	}

	@Nullable
	private Throwable shallowTraverseCauseChain(Throwable thrownException) {
		Throwable cause = thrownException;
		if (cause instanceof ListenerExecutionFailedException) {
			cause = thrownException.getCause();
		}
		return cause;
	}

	@Nullable
	private Throwable traverseCauseChain(Throwable thrownException) {
		Throwable cause = thrownException;
		while (cause != null && cause.getCause() != null) {
			if (this.classifier.classify(cause)) { // NOSONAR using Boolean here is not dangerous
				return cause;
			}
			cause = cause.getCause();
		}
		return cause;
	}

}
