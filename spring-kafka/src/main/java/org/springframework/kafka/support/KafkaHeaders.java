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

package org.springframework.kafka.support;

/**
 * The Kafka specific message headers constants.
 *
 * @author Artem Bilan
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Biju Kunjummen
 */
public abstract class KafkaHeaders {

	/**
	 * The prefix for Kafka headers.
	 */
	public static final String PREFIX = "kafka_";

	/**
	 * The prefix for Kafka headers containing 'received' values.
	 */
	public static final String RECEIVED = PREFIX + "received";

	/**
	 * The header containing the topic when sending data to Kafka.
	 */
	public static final String TOPIC = PREFIX + "topic";

	/**
	 * The header containing the record key when sending data to Kafka.
	 * @since 2.9
	 */
	public static final String KEY = PREFIX + "messageKey";

	/**
	 * The header containing the topic partition when sending data to Kafka.
	 * @since 2.0
	 */
	public static final String PARTITION = PREFIX + "partitionId";

	/**
	 * The header for the partition offset.
	 */
	public static final String OFFSET = PREFIX + "offset";

	/**
	 * The header containing the raw data received from Kafka ({@code ConsumerRecord} or
	 * {@code ConsumerRecords}). Usually used to enhance error messages.
	 */
	public static final String RAW_DATA = PREFIX + "data";

	/**
	 * The header containing the {@code RecordMetadata} object after successful send to the topic.
	 */
	public static final String RECORD_METADATA = PREFIX + "recordMetadata";

	/**
	 * The header for the {@link Acknowledgment}.
	 */
	public static final String ACKNOWLEDGMENT = PREFIX + "acknowledgment";

	/**
	 * The header for the {@code Consumer} object.
	 */
	public static final String CONSUMER = PREFIX + "consumer";

	/**
	 * The header containing the topic from which the message was received.
	 */
	public static final String RECEIVED_TOPIC = RECEIVED + "Topic";

	/**
	 * The header containing the record key from the received message.
	 * @since 2.9
	 */
	public static final String RECEIVED_KEY = RECEIVED + "MessageKey";

	/**
	 * The header containing the topic partition from the received message.
	 * @since 2.9
	 */
	public static final String RECEIVED_PARTITION = RECEIVED + "PartitionId";

	/**
	 * The header for holding the {@link org.apache.kafka.common.record.TimestampType
	 * type} of timestamp.
	 */
	public static final String TIMESTAMP_TYPE = PREFIX + "timestampType";

	/**
	 * The header for holding the timestamp of the producer record.
	 */
	public static final String TIMESTAMP = PREFIX + "timestamp";

	/**
	 * The header for holding the timestamp of the consumer record.
	 */
	public static final String RECEIVED_TIMESTAMP = PREFIX + "receivedTimestamp";

	/**
	 * The header for holding the native headers of the consumer record; only provided
	 * if no header mapper is present.
	 */
	public static final String NATIVE_HEADERS = PREFIX + "nativeHeaders";

	/**
	 * The header for a list of Maps of converted native Kafka headers. Used for batch
	 * listeners; the map at a particular list position corresponds to the data in the
	 * payload list position.
	 */
	public static final String BATCH_CONVERTED_HEADERS = PREFIX + "batchConvertedHeaders";

	/**
	 * The header containing information to correlate requests/replies.
	 * Type: byte[].
	 * @since 2.1.3
	 */
	public static final String CORRELATION_ID = PREFIX + "correlationId";

	/**
	 * The header containing the default reply topic.
	 * Type: byte[].
	 * @since 2.1.3
	 */
	public static final String REPLY_TOPIC = PREFIX + "replyTopic";

	/**
	 * The header containing a partition number on which to send the reply.
	 * Type: binary (int) in byte[].
	 * @since 2.1.3
	 */
	public static final String REPLY_PARTITION = PREFIX + "replyPartition";

	/**
	 * Exception class name for a record published sent to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_EXCEPTION_FQCN = PREFIX + "dlt-exception-fqcn";

	/**
	 * Exception cause class name for a record published sent to a dead-letter topic.
	 * @since 2.8
	 */
	public static final String DLT_EXCEPTION_CAUSE_FQCN = PREFIX + "dlt-exception-cause-fqcn";

	/**
	 * Exception stack trace for a record published to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_EXCEPTION_STACKTRACE = PREFIX + "dlt-exception-stacktrace";

	/**
	 * Exception message for a record published to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_EXCEPTION_MESSAGE = PREFIX + "dlt-exception-message";

	/**
	 * Exception stack trace for a record published to a dead-letter topic with a key
	 * deserialization exception.
	 * @since 2.7
	 */
	public static final String DLT_KEY_EXCEPTION_STACKTRACE = PREFIX + "dlt-key-exception-stacktrace";

	/**
	 * Exception message for a record published to a dead-letter topic with a key
	 * deserialization exception.
	 * @since 2.7
	 */
	public static final String DLT_KEY_EXCEPTION_MESSAGE = PREFIX + "dlt-key-exception-message";

	/**
	 * Exception class name for a record published sent to a dead-letter topic with a key
	 * deserialization exception.
	 * @since 2.7
	 */
	public static final String DLT_KEY_EXCEPTION_FQCN = PREFIX + "dlt-key-exception-fqcn";

	/**
	 * Original topic for a record published to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_ORIGINAL_TOPIC = PREFIX + "dlt-original-topic";

	/**
	 * Original partition for a record published to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_ORIGINAL_PARTITION = PREFIX + "dlt-original-partition";

	/**
	 * Original offset for a record published to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_ORIGINAL_OFFSET = PREFIX + "dlt-original-offset";

	/**
	 * Consumer group that failed to consumer a record published to a dead-letter topic.
	 * @since 2.8
	 */
	public static final String DLT_ORIGINAL_CONSUMER_GROUP = PREFIX + "dlt-original-consumer-group";

	/**
	 * Original timestamp for a record published to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_ORIGINAL_TIMESTAMP = PREFIX + "dlt-original-timestamp";

	/**
	 * Original timestamp type for a record published to a dead-letter topic.
	 * @since 2.2
	 */
	public static final String DLT_ORIGINAL_TIMESTAMP_TYPE = PREFIX + "dlt-original-timestamp-type";

	/**
	 * For inbound messages, the container's {@code group.id} consumer property.
	 * @since 2.3
	 */
	public static final String GROUP_ID = PREFIX + "groupId";

	/**
	 * For inbound messages, when container retries are enabled the delivery attempt.
	 * @since 2.5
	 */
	public static final String DELIVERY_ATTEMPT = PREFIX + "deliveryAttempt";

	/**
	 * Exception class name for a record published sent to another topic.
	 * @since 2.2
	 */
	public static final String EXCEPTION_FQCN = PREFIX + "exception-fqcn";

	/**
	 * Exception class name for a record published sent to another topic.
	 * @since 2.8
	 */
	public static final String EXCEPTION_CAUSE_FQCN = PREFIX + "exception-cause-fqcn";

	/**
	 * Exception stack trace for a record published to another topic.
	 * @since 2.2
	 */
	public static final String EXCEPTION_STACKTRACE = PREFIX + "exception-stacktrace";

	/**
	 * Exception message for a record published to another topic.
	 * @since 2.2
	 */
	public static final String EXCEPTION_MESSAGE = PREFIX + "exception-message";

	/**
	 * Exception stack trace for a record published to another topic with a key
	 * deserialization exception.
	 * @since 2.7
	 */
	public static final String KEY_EXCEPTION_STACKTRACE = PREFIX + "key-exception-stacktrace";

	/**
	 * Exception message for a record published to another topic with a key
	 * deserialization exception.
	 * @since 2.7
	 */
	public static final String KEY_EXCEPTION_MESSAGE = PREFIX + "key-exception-message";

	/**
	 * Exception class name for a record published sent to another topic with a key
	 * deserialization exception.
	 * @since 2.7
	 */
	public static final String KEY_EXCEPTION_FQCN = PREFIX + "key-exception-fqcn";

	/**
	 * Original topic for a record published to another topic.
	 * @since 2.2
	 */
	public static final String ORIGINAL_TOPIC = PREFIX + "original-topic";

	/**
	 * Original partition for a record published to another topic.
	 * @since 2.2
	 */
	public static final String ORIGINAL_PARTITION = PREFIX + "original-partition";

	/**
	 * Original offset for a record published to another topic.
	 * @since 2.2
	 */
	public static final String ORIGINAL_OFFSET = PREFIX + "original-offset";

	/**
	 * Consumer group that failed to consumer a record published to another topic.
	 * @since 2.8
	 */
	public static final String ORIGINAL_CONSUMER_GROUP = PREFIX + "dlt-original-consumer-group";

	/**
	 * Original timestamp for a record published to another topic.
	 * @since 2.2
	 */
	public static final String ORIGINAL_TIMESTAMP = PREFIX + "original-timestamp";

	/**
	 * Original timestamp type for a record published to another topic.
	 * @since 2.2
	 */
	public static final String ORIGINAL_TIMESTAMP_TYPE = PREFIX + "original-timestamp-type";

	/**
	 * The header containing a list of conversion failures (for batch listeners only).
	 * Type: List&lt;ConversionException&gt;.
	 * @since 2.8
	 */
	public static final String CONVERSION_FAILURES = PREFIX + "conversionFailures";

	/**
	 * Arbitrary static information about the listener receiving this record.
	 * @since 2.8.4
	 */
	public static final String LISTENER_INFO = PREFIX + "listenerInfo";

}
