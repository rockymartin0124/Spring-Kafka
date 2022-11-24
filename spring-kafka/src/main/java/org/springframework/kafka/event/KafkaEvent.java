/*
 * Copyright 2015-2021 the original author or authors.
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

package org.springframework.kafka.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.util.Assert;


/**
 * Base class for events.
 *
 * @author Gary Russell
 *
 */
public abstract class KafkaEvent extends ApplicationEvent {

	private static final long serialVersionUID = 1L;

	private final Object container;

	public KafkaEvent(Object source, Object container) {
		super(source);
		this.container = container;
	}

	/**
	 * Get the container for which the event was published, which will be the parent
	 * container if the source that emitted the event is a child container, or the source
	 * itself otherwise. The type is required here to avoid a dependency tangle between
	 * the event and listener packages.
	 * @param type the container type (e.g. {@code MessageListenerContainer.class}).
	 * @param <T> the type.
	 * @return the container.
	 * @since 2.2.1
	 * @see #getSource(Class)
	 */
	@SuppressWarnings("unchecked")
	public <T> T getContainer(Class<T> type) {
		Assert.isInstanceOf(type, this.container);
		return (T) this.container;
	}

	/**
	 * Get the container (source) that published the event. This is provided as an
	 * alternative to {@link #getSource()} to avoid the need to cast in user code. The
	 * type is required here to avoid a dependency tangle between the event and listener
	 * packages.
	 * @param type the container type (e.g. {@code MessageListenerContainer.class}).
	 * @param <T> the type.
	 * @return the container.
	 * @since 2.2.1
	 * @see #getContainer(Class)
	 * @see #getSource()
	 */
	@SuppressWarnings("unchecked")
	public <T> T getSource(Class<T> type) {
		Assert.isInstanceOf(type, getSource());
		return (T) getSource();
	}

}
