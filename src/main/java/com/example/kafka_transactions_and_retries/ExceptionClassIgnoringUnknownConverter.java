package com.example.kafka_transactions_and_retries;

import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
@ConfigurationPropertiesBinding
public class ExceptionClassIgnoringUnknownConverter implements Converter<String, Class<? extends Throwable>> {
	@Override
	public Class<? extends Throwable> convert(String source) {
		try {
			// TODO align class resolution with Spring’s behaviour (re-use Spring’s ClassEditor?)
			return Class.forName(source).asSubclass(Throwable.class);
		} catch (ClassNotFoundException e) {
			return UnknownException.class;
		}
	}

	private static class UnknownException extends RuntimeException {
	}
}
