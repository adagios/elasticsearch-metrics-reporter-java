/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.metrics;

import com.codahale.metrics.*;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.metrics.JsonMetrics.*;

public class MetricsElasticsearchModule extends Module {

    public static final Version VERSION = new Version(1, 0, 0, "", "metrics-elasticsearch-reporter", "metrics-elasticsearch-reporter");

    private static void writeAdditionalFields(final Map<String, Object> additionalFields, final JsonGenerator json) throws IOException {
        if (additionalFields != null) {
            for (final Map.Entry<String, Object> field : additionalFields.entrySet()) {
                json.writeObjectField(field.getKey(), field.getValue());
            }
        }
    }

    private static void writeExtractedNameParts(NamePartsExtractor[] extractors, JsonGenerator json, String name) throws IOException {
        if (extractors != null) {
            for (NamePartsExtractor extractor : extractors) {
                Map<String, Object> parts = extractor.extract(name);
                if (parts != null) writeAdditionalFields(parts, json);
            }
        }
    }

    private static abstract class AbstractMetricSerializer<T extends JsonMetric> extends StdSerializer<T> {
        private final String timestampFieldname;
        private final Map<String, Object> additionalFields;
        private NamePartsExtractor[] extractors;

        private AbstractMetricSerializer(Class type,
                                         String timestampFieldname, Map<String, Object> additionalFields,
                                NamePartsExtractor... extractors) {
            super(type);
            this.timestampFieldname = timestampFieldname;
            this.additionalFields = additionalFields;
            this.extractors = extractors;
        }

        @Override
        public void serialize(T metric,
                              JsonGenerator json,
                              SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeStringField("name", metric.name());
            json.writeObjectField(timestampFieldname, metric.timestampAsDate());

            serializeValue(metric, json, provider);

            writeAdditionalFields(additionalFields, json);
            writeExtractedNameParts(extractors, json, metric.name());

            json.writeEndObject();
        }

        public abstract void serializeValue(T metric,
                                            JsonGenerator json,
                                            SerializerProvider provider) throws IOException;
    }


    private static class GaugeSerializer extends AbstractMetricSerializer<JsonGauge> {

        private GaugeSerializer(String timestampFieldname,
                                Map<String, Object> additionalFields, NamePartsExtractor... extractors) {
            super(JsonGauge.class, timestampFieldname, additionalFields, extractors);
        }


        @Override
        public void serializeValue(JsonGauge gauge, JsonGenerator json, SerializerProvider provider) throws IOException {
            final Object value;
            try {
                value = gauge.value().getValue();
                json.writeObjectField("value", value);
            } catch (RuntimeException e) {
                json.writeObjectField("error", e.toString());
            }
        }
    }

    private static class CounterSerializer extends AbstractMetricSerializer<JsonCounter> {

        private CounterSerializer(String timestampFieldname,
                                  Map<String, Object> additionalFields, NamePartsExtractor... extractors) {
            super(JsonCounter.class, timestampFieldname, additionalFields, extractors);
        }

        @Override
        public void serializeValue(JsonCounter counter, JsonGenerator json,
                                   SerializerProvider provider) throws IOException {
            json.writeNumberField("count", counter.value().getCount());
        }
    }

    private static class HistogramSerializer extends AbstractMetricSerializer<JsonHistogram> {


        private HistogramSerializer(String timestampFieldname,
                                    Map<String, Object> additionalFields, NamePartsExtractor... extractors) {
            super(JsonHistogram.class, timestampFieldname, additionalFields, extractors);
        }

        @Override
        public void serializeValue(JsonHistogram jsonHistogram, JsonGenerator json,
                                   SerializerProvider provider) throws IOException {
            Histogram histogram = jsonHistogram.value();

            final Snapshot snapshot = histogram.getSnapshot();
            json.writeNumberField("count", histogram.getCount());
            json.writeNumberField("max", snapshot.getMax());
            json.writeNumberField("mean", snapshot.getMean());
            json.writeNumberField("min", snapshot.getMin());
            json.writeNumberField("p50", snapshot.getMedian());
            json.writeNumberField("p75", snapshot.get75thPercentile());
            json.writeNumberField("p95", snapshot.get95thPercentile());
            json.writeNumberField("p98", snapshot.get98thPercentile());
            json.writeNumberField("p99", snapshot.get99thPercentile());
            json.writeNumberField("p999", snapshot.get999thPercentile());
            json.writeNumberField("stddev", snapshot.getStdDev());
        }
    }

    private static class MeterSerializer extends AbstractMetricSerializer<JsonMeter> {
        private final String rateUnit;
        private final double rateFactor;

        private MeterSerializer(TimeUnit rateUnit, String timestampFieldname,
                                Map<String, Object> additionalFields, NamePartsExtractor... extractors) {
            super(JsonMeter.class, timestampFieldname, additionalFields, extractors);
                this.rateFactor = rateUnit.toSeconds(1);
                this.rateUnit = calculateRateUnit(rateUnit, "events");

        }

        @Override
        public void serializeValue(JsonMeter jsonMeter, JsonGenerator json, SerializerProvider provider) throws IOException {
            Meter meter = jsonMeter.value();
            json.writeNumberField("count", meter.getCount());
            json.writeNumberField("m1_rate", meter.getOneMinuteRate() * rateFactor);
            json.writeNumberField("m5_rate", meter.getFiveMinuteRate() * rateFactor);
            json.writeNumberField("m15_rate", meter.getFifteenMinuteRate() * rateFactor);
            json.writeNumberField("mean_rate", meter.getMeanRate() * rateFactor);
            json.writeStringField("units", rateUnit);
        }
    }

    private static class TimerSerializer  extends AbstractMetricSerializer<JsonTimer> {
        private final String rateUnit;
        private final double rateFactor;
        private final String durationUnit;
        private final double durationFactor;

        private TimerSerializer(TimeUnit rateUnit, TimeUnit durationUnit, String timestampFieldname,
                                Map<String, Object> additionalFields, NamePartsExtractor... extractors) {
            super(JsonTimer.class, timestampFieldname, additionalFields, extractors);
            this.rateUnit = calculateRateUnit(rateUnit, "calls");
            this.rateFactor = rateUnit.toSeconds(1);
            this.durationUnit = durationUnit.toString().toLowerCase(Locale.US);
            this.durationFactor = 1.0 / durationUnit.toNanos(1);
        }

        @Override
        public void serializeValue(JsonTimer jsonTimer, JsonGenerator json, SerializerProvider provider) throws IOException {
            Timer timer = jsonTimer.value();
            final Snapshot snapshot = timer.getSnapshot();
            json.writeNumberField("count", timer.getCount());
            json.writeNumberField("max", snapshot.getMax() * durationFactor);
            json.writeNumberField("mean", snapshot.getMean() * durationFactor);
            json.writeNumberField("min", snapshot.getMin() * durationFactor);

            json.writeNumberField("p50", snapshot.getMedian() * durationFactor);
            json.writeNumberField("p75", snapshot.get75thPercentile() * durationFactor);
            json.writeNumberField("p95", snapshot.get95thPercentile() * durationFactor);
            json.writeNumberField("p98", snapshot.get98thPercentile() * durationFactor);
            json.writeNumberField("p99", snapshot.get99thPercentile() * durationFactor);
            json.writeNumberField("p999", snapshot.get999thPercentile() * durationFactor);

            /*
            if (showSamples) {
                final long[] values = snapshot.getValues();
                final double[] scaledValues = new double[values.length];
                for (int i = 0; i < values.length; i++) {
                    scaledValues[i] = values[i] * durationFactor;
                }
                json.writeObjectField("values", scaledValues);
            }
            */

            json.writeNumberField("stddev", snapshot.getStdDev() * durationFactor);
            json.writeNumberField("m1_rate", timer.getOneMinuteRate() * rateFactor);
            json.writeNumberField("m5_rate", timer.getFiveMinuteRate() * rateFactor);
            json.writeNumberField("m15_rate", timer.getFifteenMinuteRate() * rateFactor);
            json.writeNumberField("mean_rate", timer.getMeanRate() * rateFactor);
            json.writeStringField("duration_units", durationUnit);
            json.writeStringField("rate_units", rateUnit);
        }
    }


    /**
     * Serializer for the first line of the bulk index operation before the json metric is written
     */
    private static class BulkIndexOperationHeaderSerializer extends StdSerializer<BulkIndexOperationHeader> {

        public BulkIndexOperationHeaderSerializer(String timestampFieldname, Map<String, Object> additionalFields) {
            super(BulkIndexOperationHeader.class);
        }

        @Override
        public void serialize(BulkIndexOperationHeader bulkIndexOperationHeader, JsonGenerator json, SerializerProvider provider) throws IOException {
            json.writeStartObject();
            json.writeObjectFieldStart("index");
            if (bulkIndexOperationHeader.index != null) {
                json.writeStringField("_index", bulkIndexOperationHeader.index);
            }
            if (bulkIndexOperationHeader.type != null) {
                json.writeStringField("_type", bulkIndexOperationHeader.type);
            }
            json.writeEndObject();
            json.writeEndObject();
        }
    }

    public static class BulkIndexOperationHeader {
        public String index;
        public String type;

        public BulkIndexOperationHeader(String index, String type) {
            this.index = index;
            this.type = type;
        }
    }

    private final TimeUnit rateUnit;
    private final TimeUnit durationUnit;
    private final String timestampFieldname;
    private final Map<String, Object> additionalFields;
    private NamePartsExtractor[] namePartsExtractors;

    public MetricsElasticsearchModule(TimeUnit rateUnit, TimeUnit durationUnit, String timestampFieldname,
                                      Map<String, Object> additionalFields, NamePartsExtractor[] namePartsExtractors) {
        this.rateUnit = rateUnit;
        this.durationUnit = durationUnit;
        this.timestampFieldname = timestampFieldname;
        this.additionalFields = additionalFields;
        this.namePartsExtractors = namePartsExtractors;
    }

    @Override
    public String getModuleName() {
        return "metrics-elasticsearch-serialization";
    }

    @Override
    public Version version() {
        return VERSION;
    }

    @Override
    public void setupModule(SetupContext context) {
        context.addSerializers(new SimpleSerializers(Arrays.<JsonSerializer<?>>asList(
                new GaugeSerializer(timestampFieldname, additionalFields, namePartsExtractors),
                new CounterSerializer(timestampFieldname, additionalFields, namePartsExtractors),
                new HistogramSerializer(timestampFieldname, additionalFields, namePartsExtractors),
                new MeterSerializer(rateUnit, timestampFieldname, additionalFields, namePartsExtractors),
                new TimerSerializer(rateUnit, durationUnit, timestampFieldname, additionalFields, namePartsExtractors),
                new BulkIndexOperationHeaderSerializer(timestampFieldname, additionalFields)
        )));
    }

    private static String calculateRateUnit(TimeUnit unit, String name) {
        final String s = unit.toString().toLowerCase(Locale.US);
        return name + '/' + s.substring(0, s.length() - 1);
    }
}
