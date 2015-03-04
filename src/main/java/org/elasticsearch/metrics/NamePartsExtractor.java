package org.elasticsearch.metrics;

import java.util.Map;

/**
 * Extracts additional fields from the name of a metric.
 *
 * The idea is that most metrics have some kind of structure,
 * and that we can extract some interesting information from them.
 */
public interface NamePartsExtractor {
    Map<String, Object> extract(String name);
}
