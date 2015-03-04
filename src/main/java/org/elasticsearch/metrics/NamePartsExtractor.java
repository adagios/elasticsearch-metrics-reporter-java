package org.elasticsearch.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Extracts additional fields from the name of a metric.
 *
 * The idea is that most metrics have some kind of structure,
 * and that we can extract some interesting information from them.
 */
public interface NamePartsExtractor {
    Map<String, Object> extract(String name);

    public static abstract class MemoizingExtractor implements NamePartsExtractor{
        private Map<String, Map<String, Object>> extractedParts = new HashMap<>();

        @Override
        public Map<String, Object> extract(String name) {
            if(!extractedParts.containsKey(name)){
                extractedParts.put(name, compute(name));
            }

            return extractedParts.get(name);
        }

        protected abstract Map<String, Object> compute(String name);
    }
}
