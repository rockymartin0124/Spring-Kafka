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

package org.springframework.kafka.support;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * The {@link ProducerListener} that logs exceptions thrown when sending messages.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class LoggingProducerListener<K, V> implements ProducerListener<K, V> {

	/**
	 * Default max content logged.
	 */
	public static final int DEFAULT_MAX_CONTENT_LOGGED = 100;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private boolean includeContents = true;

	private int maxContentLogged = DEFAULT_MAX_CONTENT_LOGGED;

	/**
	 * Whether the log message should include the contents (key and payload).
	 *
	 * @param includeContents true if the contents of the message should be logged
	 */
	public void setIncludeContents(boolean includeContents) {
		this.includeContents = includeContents;
	}

	/**
	 * The maximum amount of data to be logged for either key or password. As message sizes may vary and
	 * become fairly large, this allows limiting the amount of data sent to logs.
	 * Default {@value #DEFAULT_MAX_CONTENT_LOGGED}.
	 *
	 * @param maxContentLogged the maximum amount of data being logged.
	 */
	public void setMaxContentLogged(int maxContentLogged) {
		this.maxContentLogged = maxContentLogged;
	}

	@Override
	public void onError(ProducerRecord<K, V> record, @Nullable RecordMetadata recordMetadata, Exception exception) {
		this.logger.error(exception, () -> {
			StringBuffer logOutput = new StringBuffer();
			logOutput.append("Exception thrown when sending a message");
			if (this.includeContents) {
				logOutput.append(" with key='")
					.append(keyOrValue(record.key()))
					.append("'")
					.append(" and payload='")
					.append(keyOrValue(record.value()))
					.append("'");
			}
			logOutput.append(" to topic ").append(record.topic());
			if (record.partition() != null) {
				logOutput.append(" and partition ").append(recordMetadata != null
						? recordMetadata.partition()
						: record.partition());
			}
			logOutput.append(":");
			return logOutput.toString();
		});
	}

	private String keyOrValue(Object keyOrValue) {
		if (keyOrValue instanceof byte[]) {
			return "byte[" + ((byte[]) keyOrValue).length + "]";
		}
		else {
			return toDisplayString(ObjectUtils.nullSafeToString(keyOrValue), this.maxContentLogged);
		}
	}

	private String toDisplayString(String original, int maxCharacters) {
		if (original.length() <= maxCharacters) {
			return original;
		}
		return original.substring(0, maxCharacters) + "...";
	}

}
