package com.example.kafka_transactions_and_retries;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.ListenerContainerWithDlqAndRetryCustomizer;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ExceptionClassifier;
import org.springframework.util.backoff.BackOff;

import java.util.function.BiFunction;
import java.util.function.Consumer;

@Configuration
@Slf4j
public class ConsumerConfig {
	@Bean
	public Consumer<String> consumeMessage() {
		return s -> {
			log.info("Consuming {}", s);
			throw s.contains("retry") ? new UnsupportedOperationException(s) : new IllegalArgumentException(s);
		};
	}

	/**
	 * Add non-retriable exceptions directly on the AfterRollbackProcessor
	 */
	@Bean
	public ListenerContainerWithDlqAndRetryCustomizer customizeDefaultAfterRollbackProcessor() {
		log.info("Creating ListenerContainerCustomizer bean");

		return new ListenerContainerWithDlqAndRetryCustomizer() {
			@Override
			public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName, String group, BiFunction<ConsumerRecord<?, ?>,
										  Exception, TopicPartition> dlqDestinationResolver, BackOff backOff,
								  ExtendedConsumerProperties<KafkaConsumerProperties> extendedConsumerProperties) {
				log.info("Customizing container for destination: {}, group: {}", destinationName, group);

				if (container.getAfterRollbackProcessor() instanceof ExceptionClassifier classifier) {
					if (!extendedConsumerProperties.isDefaultRetryable()) {
						classifier.defaultFalse(true);
					}
					extendedConsumerProperties.getRetryableExceptions()
							.forEach((t, retry) -> {
								if (Exception.class.isAssignableFrom(t)) {
									var ex = t.asSubclass(Exception.class);
									if (retry) {
										classifier.addRetryableExceptions(ex);
									} else {
										classifier.addNotRetryableExceptions(ex);
									}
								}
							});
				}
			}

			@Override
			public void configure(AbstractMessageListenerContainer<?, ?> container, String destinationName, String
					group, BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> dlqDestinationResolver, BackOff backOff) {
			}
		};
	}
}
