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

package org.springframework.kafka.core;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

/**
 * The basic Kafka operations contract returning {@link CompletableFuture}s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * If the Kafka topic is set with {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime}
 * all send operations will use the user provided time if provided, else
 * {@link org.apache.kafka.clients.producer.KafkaProducer} will generate one
 *
 * If the topic is set with {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime}
 * then the user provided timestamp will be ignored and instead will be the
 * Kafka broker local time when the message is appended
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Biju Kunjummen
 */
public interface KafkaOperations<K, V> {

	/**
	 * Default timeout for {@link #receive(String, int, long)}.
	 */
	Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(5);

	/**
	 * Send the data to the default topic with no key or partition.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	CompletableFuture<SendResult<K, V>> sendDefault(V data);

	/**
	 * Send the data to the default topic with the provided key and no partition.
	 * @param key the key.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	CompletableFuture<SendResult<K, V>> sendDefault(K key, V data);

	/**
	 * Send the data to the default topic with the provided key and partition.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 */
	CompletableFuture<SendResult<K, V>> sendDefault(Integer partition, K key, V data);

	/**
	 * Send the data to the default topic with the provided key and partition.
	 * @param partition the partition.
	 * @param timestamp the timestamp of the record.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 * @since 1.3
	 */
	CompletableFuture<SendResult<K, V>> sendDefault(Integer partition, Long timestamp, K key, V data);

	/**
	 * Send the data to the provided topic with no key or partition.
	 * @param topic the topic.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	CompletableFuture<SendResult<K, V>> send(String topic, V data);

	/**
	 * Send the data to the provided topic with the provided key and no partition.
	 * @param topic the topic.
	 * @param key the key.
	 * @param data The data.
	 * @return a Future for the {@link SendResult}.
	 */
	CompletableFuture<SendResult<K, V>> send(String topic, K key, V data);

	/**
	 * Send the data to the provided topic with the provided key and partition.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 */
	CompletableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data);

	/**
	 * Send the data to the provided topic with the provided key and partition.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param timestamp the timestamp of the record.
	 * @param key the key.
	 * @param data the data.
	 * @return a Future for the {@link SendResult}.
	 * @since 1.3
	 */
	CompletableFuture<SendResult<K, V>> send(String topic, Integer partition, Long timestamp, K key, V data);

	/**
	 * Send the provided {@link ProducerRecord}.
	 * @param record the record.
	 * @return a Future for the {@link SendResult}.
	 * @since 1.3
	 */
	CompletableFuture<SendResult<K, V>> send(ProducerRecord<K, V> record);

	/**
	 * Send a message with routing information in message headers. The message payload
	 * may be converted before sending.
	 * @param message the message to send.
	 * @return a Future for the {@link SendResult}.
	 * @see org.springframework.kafka.support.KafkaHeaders#TOPIC
	 * @see org.springframework.kafka.support.KafkaHeaders#PARTITION
	 * @see org.springframework.kafka.support.KafkaHeaders#KEY
	 */
	CompletableFuture<SendResult<K, V>> send(Message<?> message);

	/**
	 * See {@link Producer#partitionsFor(String)}.
	 * @param topic the topic.
	 * @return the partition info.
	 * @since 1.1
	 */
	List<PartitionInfo> partitionsFor(String topic);

	/**
	 * See {@link Producer#metrics()}.
	 * @return the metrics.
	 * @since 1.1
	 */
	Map<MetricName, ? extends Metric> metrics();

	/**
	 * Execute some arbitrary operation(s) on the producer and return the result.
	 * @param callback the callback.
	 * @param <T> the result type.
	 * @return the result.
	 * @since 1.1
	 */
	@Nullable
	<T> T execute(ProducerCallback<K, V, T> callback);

	/**
	 * Execute some arbitrary operation(s) on the operations and return the result.
	 * The operations are invoked within a local transaction and do not participate
	 * in a global transaction (if present).
	 * @param callback the callback.
	 * @param <T> the result type.
	 * @return the result.
	 * @since 1.1
	 */
	@Nullable
	<T> T executeInTransaction(OperationsCallback<K, V, T> callback);

	/**
	 * Flush the producer.
	 */
	void flush();

	/**
	 * When running in a transaction, send the consumer offset(s) to the transaction. It
	 * is not necessary to call this method if the operations are invoked on a listener
	 * container thread (and the listener container is configured with a
	 * {@link org.springframework.kafka.transaction.KafkaAwareTransactionManager}) since
	 * the container will take care of sending the offsets to the transaction.
	 * Use with 2.5 brokers or later.
	 * @param offsets The offsets.
	 * @param groupMetadata the consumer group metadata.
	 * @since 2.5
	 * @see Producer#sendOffsetsToTransaction(Map, ConsumerGroupMetadata)
	 */
	default void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
			ConsumerGroupMetadata groupMetadata) {

		throw new UnsupportedOperationException();
	}

	/**
	 * Return true if the implementation supports transactions (has a transaction-capable
	 * producer factory).
	 * @return true or false.
	 * @since 2.3
	 */
	boolean isTransactional();

	/**
	 * Return true if this template, when transactional, allows non-transactional operations.
	 * @return true to allow.
	 * @since 2.4.3
	 */
	default boolean isAllowNonTransactional() {
		return false;
	}

	/**
	 * Return true if the template is currently running in a transaction on the calling
	 * thread.
	 * @return true if a transaction is running.
	 * @since 2.5
	 */
	default boolean inTransaction() {
		return false;
	}

	/**
	 * Return the producer factory used by this template.
	 * @return the factory.
	 * @since 2.5
	 */
	default ProducerFactory<K, V> getProducerFactory() {
		throw new UnsupportedOperationException("This implementation does not support this operation");
	}

	/**
	 * Receive a single record with the default poll timeout (5 seconds).
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param offset the offset.
	 * @return the record or null.
	 * @since 2.8
	 * @see #DEFAULT_POLL_TIMEOUT
	 */
	@Nullable
	default ConsumerRecord<K, V> receive(String topic, int partition, long offset) {
		return receive(topic, partition, offset, DEFAULT_POLL_TIMEOUT);
	}

	/**
	 * Receive a single record.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param offset the offset.
	 * @param pollTimeout the timeout.
	 * @return the record or null.
	 * @since 2.8
	 */
	@Nullable
	ConsumerRecord<K, V> receive(String topic, int partition, long offset, Duration pollTimeout);

	/**
	 * Receive a multiple records with the default poll timeout (5 seconds). Only
	 * absolute, positive offsets are supported.
	 * @param requested a collection of record requests (topic/partition/offset).
	 * @return the records
	 * @since 2.8
	 * @see #DEFAULT_POLL_TIMEOUT
	 */
	default ConsumerRecords<K, V> receive(Collection<TopicPartitionOffset> requested) {
		return receive(requested, DEFAULT_POLL_TIMEOUT);
	}

	/**
	 * Receive multiple records. Only absolute, positive offsets are supported.
	 * @param requested a collection of record requests (topic/partition/offset).
	 * @param pollTimeout the timeout.
	 * @return the record or null.
	 * @since 2.8
	 */
	ConsumerRecords<K, V> receive(Collection<TopicPartitionOffset> requested, Duration pollTimeout);

	/**
	 * A callback for executing arbitrary operations on the {@link Producer}.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @param <T> the return type.
	 * @since 1.3
	 */
	interface ProducerCallback<K, V, T> {

		T doInKafka(Producer<K, V> producer);

	}

	/**
	 * A callback for executing arbitrary operations on the {@link KafkaOperations}.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @param <T> the return type.
	 * @since 1.3
	 */
	interface OperationsCallback<K, V, T> {

		T doInOperations(KafkaOperations<K, V> operations);

	}

}
