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

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.RetryableTopicAnnotationProcessor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.retry.annotation.Backoff;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
@SuppressWarnings("deprecation")
@ExtendWith(MockitoExtension.class)
class RetryableTopicAnnotationProcessorTests {

	private final String topic1 = "topic1";

	private final String topic2 = "topic2";

	private final String[] topics = {topic1, topic2};

	private static final String kafkaTemplateName = "kafkaTemplateBean";

	private final String listenerMethodName = "listenWithRetry";

	@Mock
	private KafkaOperations<?, ?> kafkaOperationsFromTemplateName;

	@Mock
	private KafkaOperations<?, ?> kafkaOperationsFromDefaultName;

	private ConfigurableBeanFactory beanFactory;

	{
		this.beanFactory = mock(ConfigurableBeanFactory.class);
		willAnswer(invoc -> {
			return invoc.getArgument(0);
		}).given(this.beanFactory).resolveEmbeddedValue(anyString());
	}

	// Retry with DLT
	private final Method listenWithRetryAndDlt = ReflectionUtils
			.findMethod(RetryableTopicAnnotationFactoryWithDlt.class, listenerMethodName);

	private final RetryableTopic annotationWithDlt = AnnotationUtils.findAnnotation(listenWithRetryAndDlt,
			RetryableTopic.class);

	private final Object beanWithDlt = createBean();

	// Retry without DLT
	private final Method listenWithRetry = ReflectionUtils.findMethod(RetryableTopicAnnotationFactory.class,
			listenerMethodName);

	private final RetryableTopic annotation = AnnotationUtils.findAnnotation(listenWithRetry, RetryableTopic.class);

	private final Object bean = createBean();

	private Object createBean() {
		try {
			return RetryableTopicAnnotationFactory.class.getDeclaredConstructor().newInstance();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void shouldGetDltHandlerMethod() {

		// setup
		given(beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		Method method = (Method) ReflectionTestUtils.getField(dltHandlerMethod, "method");
		assertThat(method.getName()).isEqualTo("handleDlt");

		assertThat(new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure()).isFalse();
	}

	@Test
	void shouldGetLoggingDltHandlerMethod() {

		// setup
		given(beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor.processAnnotation(topics, listenWithRetry, annotation, bean);

		// then
		EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		dltHandlerMethod.resolveBean(this.beanFactory);
		Method method = dltHandlerMethod.getMethod();
		assertThat(method.getName())
				.isEqualTo(RetryTopicConfigurer.LoggingDltListenerHandlerMethod.DEFAULT_DLT_METHOD_NAME);

		assertThat(new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure()).isTrue();
	}

	@Test
	void shouldThrowIfProvidedKafkaTemplateNotFound() {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).willThrow(NoSuchBeanDefinitionException.class);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		assertThatExceptionOfType(BeanInitializationException.class)
				.isThrownBy(() -> processor.processAnnotation(topics, listenWithRetry, annotation, bean));
	}

	@Test
	void shouldThrowIfNoKafkaTemplateFound() {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willThrow(NoSuchBeanDefinitionException.class);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		ObjectProvider<KafkaOperations> templateProvider = mock(ObjectProvider.class);
		given(templateProvider.getIfUnique()).willReturn(null);
		given(this.beanFactory.getBeanProvider(KafkaOperations.class))
				.willReturn(templateProvider);

		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		assertThatIllegalStateException().isThrownBy(() ->
				processor.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt));
	}

	@Test
	void shouldTrySpringBootDefaultKafkaTemplate() {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willThrow(NoSuchBeanDefinitionException.class);
		@SuppressWarnings({ "unchecked", "rawtypes" })
		ObjectProvider<KafkaOperations> templateProvider = mock(ObjectProvider.class);
		given(templateProvider.getIfUnique()).willReturn(kafkaOperationsFromDefaultName);
		given(this.beanFactory.getBeanProvider(KafkaOperations.class))
				.willReturn(templateProvider);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		RetryTopicConfiguration configuration = processor.processAnnotation(topics, listenWithRetry, annotationWithDlt,
				bean);
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertThat(destinationTopic.getKafkaOperations()).isEqualTo(kafkaOperationsFromDefaultName);
	}

	@Test
	void shouldGetKafkaTemplateFromBeanName() {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class))
				.willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetry, annotation, bean);
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertThat(destinationTopic.getKafkaOperations()).isEqualTo(kafkaOperationsFromTemplateName);
	}

	@Test
	void shouldGetKafkaTemplateFromDefaultBeanName() {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertThat(destinationTopic.getKafkaOperations()).isEqualTo(kafkaOperationsFromDefaultName);
	}

	@Test
	void shouldCreateExponentialBackoff() {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertThat(destinationTopic.getDestinationDelay()).isEqualTo(0);
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertThat(destinationTopic2.getDestinationDelay()).isEqualTo(1000);
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertThat(destinationTopic3.getDestinationDelay()).isEqualTo(2000);
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertThat(destinationTopic4.getDestinationDelay()).isEqualTo(0);

		assertThat(destinationTopic.shouldRetryOn(1, new IllegalStateException())).isFalse();
		assertThat(destinationTopic.shouldRetryOn(1, new IllegalArgumentException())).isTrue();
	}

	@Test
	void shouldSetAbort() {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertThat(destinationTopic.getDestinationDelay()).isEqualTo(0);
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertThat(destinationTopic2.getDestinationDelay()).isEqualTo(1000);
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertThat(destinationTopic3.getDestinationDelay()).isEqualTo(2000);
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertThat(destinationTopic4.getDestinationDelay()).isEqualTo(0);

	}

	@Test
	void shouldCreateFixedBackoff() {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class))
				.willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetry, annotation, bean);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertThat(destinationTopic.getDestinationDelay()).isEqualTo(0);
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertThat(destinationTopic2.getDestinationDelay()).isEqualTo(1000);
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertThat(destinationTopic3.getDestinationDelay()).isEqualTo(1000);
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertThat(destinationTopic4.getDestinationDelay()).isEqualTo(0);

	}

	static class RetryableTopicAnnotationFactory {

		@KafkaListener
		@RetryableTopic(kafkaTemplate = RetryableTopicAnnotationProcessorTests.kafkaTemplateName)
		void listenWithRetry() {
			// NoOps
		}
	}

	static class RetryableTopicAnnotationFactoryWithDlt {

		@KafkaListener
		@RetryableTopic(attempts = "3", backoff = @Backoff(multiplier = 2, value = 1000),
			dltStrategy = DltStrategy.FAIL_ON_ERROR, excludeNames = "java.lang.IllegalStateException")
		void listenWithRetry() {
			// NoOps
		}

		@DltHandler
		void handleDlt() {
			// NoOps
		}
	}
}
