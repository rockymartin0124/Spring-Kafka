/*
 * Copyright 2015-2022 the original author or authors.
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

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Handles errors thrown during the execution of a {@link MessageListener}.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @deprecated in favor of {@link CommonErrorHandler}.
 */
@Deprecated(since = "2.8", forRemoval = true) // in 3.1
public interface ErrorHandler extends GenericErrorHandler<ConsumerRecord<?, ?>> {

	/**
	 * Handle the exception.
	 * @param thrownException the exception.
	 * @param records the remaining records including the one that failed.
	 * @param consumer the consumer.
	 * @param container the container.
	 */
	default void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
			MessageListenerContainer container) {
		handle(thrownException, null);
	}

}
