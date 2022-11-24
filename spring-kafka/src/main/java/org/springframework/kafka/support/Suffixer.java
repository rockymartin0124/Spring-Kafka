/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Collection;
import java.util.stream.Collectors;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;


/**
 * Utility class that suffixes strings.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class Suffixer {

	private final String suffix;

	public Suffixer(String suffix) {
		Assert.notNull(suffix, "Suffix cannot be null");
		this.suffix = suffix;
	}

	public String maybeAddTo(String source) {
		if (!StringUtils.hasText(this.suffix)) {
			return source;
		}
		return source != null && StringUtils.hasText(source) // Only suffix if there's text
				? source.concat(this.suffix)
				: source;
	}

	public Collection<String> maybeAddTo(Collection<String> sources) {
		if (!StringUtils.hasText(this.suffix)) {
			return sources;
		}
		return sources
				.stream()
				.map(source -> maybeAddTo(source))
				.collect(Collectors.toList());
	}
}
