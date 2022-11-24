/*
 * Copyright 2017-2022 the original author or authors.
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

package org.springframework.kafka.support.converter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Biju Kunjummen
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 1.3
 */
public class BatchMessageConverterTests {

	@Test
	void testBatchConverters() {
		BatchMessageConverter batchMessageConverter = new BatchMessagingMessageConverter();

		MessageHeaders headers = testGuts(batchMessageConverter);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> converted = (List<Map<String, Object>>) headers
				.get(KafkaHeaders.BATCH_CONVERTED_HEADERS);
		assertThat(converted).hasSize(3);
		Map<String, Object> map = converted.get(0);
		assertThat(map).hasSize(1);
		assertThat(new String((byte[]) map.get("foo"))).isEqualTo("bar");
		assertThat(headers.get(KafkaHeaders.RAW_DATA)).isNull();
	}

	@Test
	void testNoMapper() {
		BatchMessagingMessageConverter batchMessageConverter = new BatchMessagingMessageConverter();
		batchMessageConverter.setHeaderMapper(null);

		MessageHeaders headers = testGuts(batchMessageConverter);
		@SuppressWarnings("unchecked")
		List<Headers> natives = (List<Headers>) headers.get(KafkaHeaders.NATIVE_HEADERS);
		assertThat(natives).hasSize(3);
		Iterator<Header> iterator = natives.get(0).iterator();
		assertThat(iterator.hasNext()).isEqualTo(true);
		Header next = iterator.next();
		assertThat(next.key()).isEqualTo("foo");
		assertThat(new String(next.value())).isEqualTo("bar");
	}

	@Test
	void raw() {
		BatchMessagingMessageConverter batchMessageConverter = new BatchMessagingMessageConverter();
		batchMessageConverter.setRawRecordHeader(true);
		MessageHeaders headers = testGuts(batchMessageConverter);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> converted = (List<Map<String, Object>>) headers
				.get(KafkaHeaders.BATCH_CONVERTED_HEADERS);
		assertThat(converted).hasSize(3);
		Map<String, Object> map = converted.get(0);
		assertThat(map).hasSize(1);
		assertThat(new String((byte[]) map.get("foo"))).isEqualTo("bar");
		@SuppressWarnings("unchecked")
		List<ConsumerRecord<?, ?>> rawHeader = headers.get(KafkaHeaders.RAW_DATA, List.class);
		assertThat(rawHeader).extracting(rec -> (String) rec.value())
				.containsExactly("value1", "value2", "value3");
	}

	private MessageHeaders testGuts(BatchMessageConverter batchMessageConverter) {
		List<ConsumerRecord<?, ?>> consumerRecords = recordList();


		Acknowledgment ack = mock(Acknowledgment.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		KafkaUtils.setConsumerGroupId("test.g");
		Message<?> message = batchMessageConverter.toMessage(consumerRecords, ack, consumer, String.class);

		assertThat(message.getPayload())
				.isEqualTo(Arrays.asList("value1", "value2", "value3"));

		MessageHeaders headers = message.getHeaders();
		assertThat(headers.get(KafkaHeaders.RECEIVED_TOPIC))
				.isEqualTo(Arrays.asList("topic1", "topic1", "topic1"));
		assertThat(headers.get(KafkaHeaders.RECEIVED_KEY))
				.isEqualTo(Arrays.asList("key1", "key2", "key3"));
		assertThat(headers.get(KafkaHeaders.RECEIVED_PARTITION))
				.isEqualTo(Arrays.asList(0, 0, 0));
		assertThat(headers.get(KafkaHeaders.OFFSET)).isEqualTo(Arrays.asList(1L, 2L, 3L));
		assertThat(headers.get(KafkaHeaders.TIMESTAMP_TYPE))
				.isEqualTo(Arrays.asList("CREATE_TIME", "CREATE_TIME", "CREATE_TIME"));
		assertThat(headers.get(KafkaHeaders.RECEIVED_TIMESTAMP))
				.isEqualTo(Arrays.asList(1487694048607L, 1487694048608L, 1487694048609L));
		assertThat(headers.get(KafkaHeaders.ACKNOWLEDGMENT)).isSameAs(ack);
		assertThat(headers.get(KafkaHeaders.CONSUMER)).isSameAs(consumer);
		assertThat(headers.get(KafkaHeaders.GROUP_ID)).isEqualTo("test.g");
		KafkaUtils.clearConsumerGroupId();
		return headers;
	}

	private List<ConsumerRecord<?, ?>> recordList() {
		Header header = new RecordHeader("foo", "bar".getBytes());
		Headers kHeaders = new RecordHeaders(new Header[] { header });
		List<ConsumerRecord<?, ?>> consumerRecords = new ArrayList<>();
		consumerRecords.add(new ConsumerRecord<>("topic1", 0, 1, 1487694048607L,
				TimestampType.CREATE_TIME, 2, 3, "key1", "value1", kHeaders, Optional.empty()));
		consumerRecords.add(new ConsumerRecord<>("topic1", 0, 2, 1487694048608L,
				TimestampType.CREATE_TIME, 2, 3, "key2", "value2", kHeaders, Optional.empty()));
		consumerRecords.add(new ConsumerRecord<>("topic1", 0, 3, 1487694048609L,
				TimestampType.CREATE_TIME, 2, 3, "key3", "value3", kHeaders, Optional.empty()));
		return consumerRecords;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void missingHeaders() {
		BatchMessageConverter converter = new BatchMessagingMessageConverter();
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, "bar", "baz",
				new RecordHeaders(), Optional.empty());
		List<ConsumerRecord<?, ?>> records = Collections.singletonList(record);
		Message<?> message = converter.toMessage(records, null, null, null);
		assertThat(((List<String>) message.getPayload())).contains("baz");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC, List.class)).contains("foo");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_KEY, List.class)).contains("bar");
	}

}
