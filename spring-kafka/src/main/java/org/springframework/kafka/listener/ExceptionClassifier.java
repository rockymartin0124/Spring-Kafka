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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.util.Assert;

/**
 * Supports exception classification.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public abstract class ExceptionClassifier extends KafkaExceptionLogLevelAware {

	private ExtendedBinaryExceptionClassifier classifier;

	/**
	 * Construct the instance.
	 */
	public ExceptionClassifier() {
		this.classifier = configureDefaultClassifier(true);
	}

	/**
	 * Return a list of the framework default fatal exceptions.
	 * This method produces a new list for each call, so changing the list's
	 * contents has no effect on the framework itself.
	 * Thus, it should be used only as a reference.
	 * @return the default fatal exceptions list.
	 */
	public static List<Class<? extends Throwable>> defaultFatalExceptionsList() {
		return Arrays.asList(DeserializationException.class,
							MessageConversionException.class,
							ConversionException.class,
							MethodArgumentResolutionException.class,
							NoSuchMethodException.class,
							ClassCastException.class);
	}

	private static ExtendedBinaryExceptionClassifier configureDefaultClassifier(boolean defaultClassification) {
		return new ExtendedBinaryExceptionClassifier(defaultFatalExceptionsList().stream()
				.collect(Collectors.toMap(ex -> ex, ex -> false)), defaultClassification);
	}

	/**
	 * By default, unmatched types classify as true. Call this method to make the default
	 * false, and optionally retain types implicitly classified as false. This should be
	 * called before calling any of the classification modification methods. This can be
	 * useful if you want to classify a super class of one or more of the standard fatal
	 * exceptions as retryable.
	 * @param retainStandardFatal true to retain.
	 * @since 3.0
	 */
	public void defaultFalse(boolean retainStandardFatal) {
		if (retainStandardFatal) {
			this.classifier = configureDefaultClassifier(false);
		}
		else {
			defaultFalse();
		}
	}

	/**
	 * By default, unmatched types classify as true. Call this method to make the default
	 * false, and remove types explicitly classified as false. This should be called before
	 * calling any of the classification modification methods.
	 * @since 2.8.4
	 */
	public void defaultFalse() {
		this.classifier = new ExtendedBinaryExceptionClassifier(new HashMap<>(), false);
	}

	/**
	 * Return the exception classifier.
	 * @return the classifier.
	 */
	protected BinaryExceptionClassifier getClassifier() {
		return this.classifier;
	}

	/**
	 * Set an exception classifications to determine whether the exception should cause a retry
	 * (until exhaustion) or not. If not, we go straight to the recoverer. By default,
	 * the following exceptions will not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link ConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * When calling this method, the defaults will not be applied.
	 * @param classifications the classifications.
	 * @param defaultValue whether or not to retry non-matching exceptions.
	 * @see BinaryExceptionClassifier#BinaryExceptionClassifier(Map, boolean)
	 * @see #addNotRetryableExceptions(Class...)
	 */
	public void setClassifications(Map<Class<? extends Throwable>, Boolean> classifications, boolean defaultValue) {
		Assert.notNull(classifications, "'classifications' + cannot be null");
		this.classifier = new ExtendedBinaryExceptionClassifier(classifications, defaultValue);
	}

	/**
	 * Add exception types to the default list. By default, the following exceptions will
	 * not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link ConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried, unless {@link #defaultFalse()} has been called.
	 * @param exceptionTypes the exception types.
	 * @see #removeClassification(Class)
	 * @see #setClassifications(Map, boolean)
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public final void addNotRetryableExceptions(Class<? extends Exception>... exceptionTypes) {
		add(false, exceptionTypes);
		notRetryable(Arrays.stream(exceptionTypes));
	}

	/**
	 * Subclasses can override this to receive notification of configuration of not
	 * retryable exceptions.
	 * @param notRetryable the not retryable exceptions.
	 * @since 2.9.3
	 */
	protected void notRetryable(Stream<Class<? extends Exception>> notRetryable) {
	}

	/**
	 * Add exception types that can be retried. Call this after {@link #defaultFalse()} to
	 * specify those exception types that should be classified as true.
	 * All others will be retried, unless {@link #defaultFalse()} has been called.
	 * @param exceptionTypes the exception types.
	 * @since 2.8.4
	 * @see #removeClassification(Class)
	 * @see #setClassifications(Map, boolean)
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public final void addRetryableExceptions(Class<? extends Exception>... exceptionTypes) {
		add(true, exceptionTypes);
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	private void add(boolean classified, Class<? extends Exception>... exceptionTypes) {
		Assert.notNull(exceptionTypes, "'exceptionTypes' cannot be null");
		Assert.noNullElements(exceptionTypes, "'exceptionTypes' cannot contain nulls");
		for (Class<? extends Exception> exceptionType : exceptionTypes) {
			Assert.isTrue(Exception.class.isAssignableFrom(exceptionType),
					() -> "exceptionType " + exceptionType + " must be an Exception");
			this.classifier.getClassified().put(exceptionType, classified);
		}
	}

	/**
	 * Remove an exception type from the configured list. By default, the following
	 * exceptions will not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link ConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried, unless {@link #defaultFalse()} has been called.
	 * @param exceptionType the exception type.
	 * @return the classification of the exception if removal was successful;
	 * null otherwise.
	 * @since 2.8.4
	 * @see #addNotRetryableExceptions(Class...)
	 * @see #setClassifications(Map, boolean)
	 */
	@Nullable
	public Boolean removeClassification(Class<? extends Exception> exceptionType) {
		return this.classifier.getClassified().remove(exceptionType);
	}

	/**
	 * Extended to provide visibility to the current classified exceptions.
	 *
	 * @author Gary Russell
	 *
	 */
	@SuppressWarnings("serial")
	private static final class ExtendedBinaryExceptionClassifier extends BinaryExceptionClassifier {

		ExtendedBinaryExceptionClassifier(Map<Class<? extends Throwable>, Boolean> typeMap, boolean defaultValue) {
			super(typeMap, defaultValue);
			setTraverseCauses(true);
		}

		@Override
		protected Map<Class<? extends Throwable>, Boolean> getClassified() { // NOSONAR worthless override
			return super.getClassified();
		}

	}

}
