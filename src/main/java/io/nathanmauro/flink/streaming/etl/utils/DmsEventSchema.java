/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.nathanmauro.flink.streaming.etl.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nathanmauro.flink.streaming.etl.events.DmsEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SuppressWarnings("DuplicatedCode")
public class DmsEventSchema implements SerializationSchema<DmsEvent>, DeserializationSchema<DmsEvent> {

    private final static ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(DmsEventSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    }

    public static String toElasticIndex(DmsEvent event) throws JsonProcessingException {
        String location = getElasticGeopPoint(event);
        LOG.info("Event Location: " + event.getMetadata().get("table-name") + "|" + location);

        return "{" +
                mapper.writeValueAsString(event.get("data")).replaceAll("^\\{|}$", "") +
                ", " +
                location +
                ", " +
                "\"metadata\": " +
                mapper.writeValueAsString(event.get("metadata")) +
                "}";
    }

    private static String getElasticGeopPoint(DmsEvent event) {
        Map<CharSequence, Object> data = event.getData();
        StringBuilder builder = new StringBuilder();
        builder.append("\"location\": {");
        List<CharSequence> latLonKeys = data.keySet().stream()
                .filter(cs -> cs.toString().contains("LAT") || cs.toString().contains("LON"))
                .collect(Collectors.toList());
        String latLonFields = latLonKeys.stream()
                .map(cs -> cs.toString().contains("LAT") ? "\"lat\": " + data.get(cs) : "\"lon\": " + data.get(cs))
                .collect(Collectors.joining(","));
        builder.append(latLonFields);
        builder.append("}");
//        "2022-03-09T05:00:00.000-00:00"
//        "2022-03-09T05:00:00.000-00:00"
//        2022-03-10 09:11:21

        return builder.toString();
    }

    public static String toJson(DmsEvent event) throws JsonProcessingException {

        return "{" +
                "\"metadata\": " +
                mapper.writeValueAsString(event.get("metadata")) +
                ", " +
                "\"data\": " +
                mapper.writeValueAsString(event.get("data")) +
                "}";
    }

    private static void addField(StringBuilder builder, DmsEvent event, String fieldName) {
        addField(builder, fieldName, event.get(fieldName));
    }

    private static void addField(StringBuilder builder, String fieldName, Object value) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append(value);
    }

//  public static String toElasticIndex(DmsEvent event) throws JsonProcessingException {
//    return mapper.writeValueAsString(event.get("data"));
//  }

    private static void addTextField(StringBuilder builder, DmsEvent event, String fieldName) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append("\"");
        builder.append(event.get(fieldName));
        builder.append("\"");
    }

    @Override
    public byte[] serialize(DmsEvent event) {
        try {
            return toJson(event).getBytes();
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public DmsEvent deserialize(byte[] bytes) {
        try {
            ObjectNode node = mapper.readValue(bytes, ObjectNode.class);

            JsonNode metadataNode = node.get("metadata");
            JsonNode dataNode = node.get("data");

            Map<CharSequence, CharSequence> metadata = mapper.convertValue(metadataNode, new TypeReference<>() {
            });
            Map<CharSequence, Object> data = mapper.convertValue(dataNode, new TypeReference<>() {
            });

            return DmsEvent
                    .newBuilder()
                    .setData(data == null ? new HashMap<>() : data)
                    .setMetadata(metadata)
                    .build();
        }
        catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);

            return null;
        }
    }

    @Override
    public boolean isEndOfStream(DmsEvent DmsEvent) {
        return false;
    }

    @Override
    public TypeInformation<DmsEvent> getProducedType() {
        return new AvroTypeInfo<>(DmsEvent.class);
    }
}
