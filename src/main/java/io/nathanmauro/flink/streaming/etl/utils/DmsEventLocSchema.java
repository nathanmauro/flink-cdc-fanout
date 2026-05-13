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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nathanmauro.flink.streaming.etl.events.DmsEventLoc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SuppressWarnings("DuplicatedCode")
public class DmsEventLocSchema implements SerializationSchema<DmsEventLoc>, DeserializationSchema<DmsEventLoc> {

    private final static ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(DmsEventLocSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    }

    public static XContentBuilder getXContentJson(DmsEventLoc event) throws IOException {
        Map<CharSequence, Object> eventData = event.getData();
        Map<CharSequence, CharSequence> eventMetadata = event.getMetadata();

        Map<String, Object> dataMap = new HashMap<>(eventData.size());
        Map<String, Object> metadataMap = new HashMap<>(eventData.size());

        eventData.forEach((key, value) -> dataMap.put(key.toString(), value));
        eventMetadata.forEach((key, value) -> metadataMap.put(key.toString(), value));

        List<Float> eventLocation = event.getLocation();

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        buildEsData(dataMap, builder);
        if (!eventLocation.isEmpty()) {
            builder.latlon("location", eventLocation.get(0), eventLocation.get(1));
        }
        builder.field("metadata", metadataMap);

        return builder.endObject();
    }

    public static XContentBuilder getEsMapping(DmsEventLoc event) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        {
            builder.startObject("properties");
            {
                buildEsMapForDataNode(builder, event);
                builder.startObject("location");
                {
                    builder.field("type", "geo_point");
                }
                builder.endObject();
                builder.startObject("metadata");
                {
                    builder.startObject("properties");
                    {
                        buildEsMapForMetadataNode(builder, event);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    private static void buildEsMapForDataNode(XContentBuilder builder, DmsEventLoc event) {
        Map<CharSequence, Object> eventData = event.getData();
        Map<String, Object> dataMap = new HashMap<>(eventData.size());
        eventData.forEach((key, value) -> dataMap.put(key.toString(), value));

        dataMap.forEach((name, value) -> {
            try {
                builder.startObject(name);
                {
                    String type = getEsType(value);

                    builder.field("type", type);

                    if (type.equals("text")) {
                        builder.startObject("fields");
                        {
                            builder.startObject("keyword");
                            {
                                builder.field("type", "keyword");
                                if (String.valueOf(value).length() <= 256) {
                                    builder.field("ignore_above", 256);
                                }
                            }
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static void buildEsMapForMetadataNode(XContentBuilder builder, DmsEventLoc event) {
        Map<CharSequence, CharSequence> eventMetadata = event.getMetadata();
        Map<String, Object> metadataMap = new HashMap<>(eventMetadata.size());
        eventMetadata.forEach((key, value) -> metadataMap.put(key.toString(), value));

        metadataMap.forEach((name, value) -> {
            try {
                builder.startObject(name);
                {
                    builder.field("type", "text");
                    builder.startObject("fields");
                    {
                        builder.startObject("keyword");
                        {
                            builder.field("type", "keyword");
                            builder.field("ignore_above", 256);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private static String getEsType(Object value) {
        String type = "";
        if (isValidDate(value)) {
            type = "date";
        }
        else if (value instanceof Integer) {
            type = "long";
        }
        else if (value instanceof String) {
            type = "text";
        }
        else if (value instanceof Long) {
            type = "long";
        }
        else if (value instanceof Float) {
            type = "float";
        }
        else if (value instanceof Double) {
            type = "float";
        }
        else {
            type = "text";
        }
        return type;
    }

    private static boolean isValidDate(Object value) {
        try {
            Instant.parse(String.valueOf(value));
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private static void buildEsData(Map<String, Object> dataMap, XContentBuilder builder) {
        dataMap.forEach((name, value) -> {
            try {
                builder.field(name, value);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static String toElasticIndex(DmsEventLoc event) throws JsonProcessingException {
        List<Float> eventLocation = event.getLocation();
        String location = null;
        if (!eventLocation.isEmpty()) {
            location = mapper.writeValueAsString(eventLocation);
            LOG.info("Event Location: " + event.getMetadata().get("table-name") + "|" + location);
        }

        String dataString = event.getData().size() > 0
                ? mapper.writeValueAsString(event.get("data")).replaceAll("^\\{|}$", "")
                : "\"data\": null";

        return "{" +
                dataString +
                ", " +
                "\"location\": " + location +
                ", " +
                "\"metadata\": " +
                mapper.writeValueAsString(event.get("metadata")) +
                "}";
    }

    public static String toJson(DmsEventLoc event) throws JsonProcessingException {

        return "{" +
                "\"metadata\": " +
                mapper.writeValueAsString(event.get("metadata")) +
                ", " +
                "\"data\": " +
                mapper.writeValueAsString(event.get("data")) +
                "}";
    }

    private static void addField(StringBuilder builder, DmsEventLoc event, String fieldName) {
        addField(builder, fieldName, event.get(fieldName));
    }

    private static void addField(StringBuilder builder, String fieldName, Object value) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append(value);
    }

    private static void addTextField(StringBuilder builder, DmsEventLoc event, String fieldName) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append("\"");
        builder.append(event.get(fieldName));
        builder.append("\"");
    }

    private static String getElasticGeopPoint(Map<CharSequence, Object> data) {
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
        return builder.toString();
    }

    @Override
    public byte[] serialize(DmsEventLoc event) {
        try {
            return toJson(event).getBytes();
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public DmsEventLoc deserialize(byte[] bytes) {
        try {
            ObjectNode node = mapper.readValue(bytes, ObjectNode.class);

            JsonNode metadataNode = node.get("metadata");
            JsonNode dataNode = node.get("data");

            Map<CharSequence, CharSequence> metadata = mapper.convertValue(metadataNode, new TypeReference<>() {
            });
            Map<CharSequence, Object> data = mapper.convertValue(dataNode, new TypeReference<>() {
            });

            List<Float> geoLatLon = new ArrayList<>(2);
            if (data != null) {
                List<Map.Entry<CharSequence, Object>> latLonList = data.entrySet().stream().filter(o -> {
                    String key = o.getKey().toString().toLowerCase();
                    boolean probLatLon = key.contains("lat") || key.contains("lon");

                    if (probLatLon) {
                        try {
                            Float.parseFloat(o.getValue().toString());
                            return true;
                        }
                        catch (Exception e) {
                            return false;
                        }
                    }
                    return false;
                }).collect(Collectors.toList());
                if (!latLonList.isEmpty()) {
                    Stream<Map.Entry<CharSequence, Object>> sorted = latLonList.stream()
                            .sorted((o1, o2) -> {
                                String o1s = o1.getKey().toString();
                                String o2s = o2.getKey().toString();
                                String lat = o1s.contains("LAT") ? o1s : o2s;
                                String lon = o2s.contains("LON") ? o2s : o1s;
                                String cLat = lat.substring(lat.indexOf("LAT"), 3);
                                String cLon = lon.substring(lon.indexOf("LON"), 3);

                                return cLon.compareToIgnoreCase(cLat);
                            });
                    geoLatLon = sorted
                            .filter(cso -> {
                                float f = Float.parseFloat(cso.getValue().toString());
                                String key = cso.getKey().toString();

                                return key.contains("LAT")
                                        ? Float.isFinite(f) && Math.abs(f) <= 90
                                        : Float.isFinite(f) && Math.abs(f) <= 180;
                            })
                            .map(cso -> Float.parseFloat(cso.getValue().toString())).collect(Collectors.toList());
                }
            }

            return DmsEventLoc
                    .newBuilder()
                    .setData(data == null ? new HashMap<>() : data)
                    .setLocation(geoLatLon.size() > 1 ? geoLatLon : Arrays.asList(0f, 0f))
                    .setMetadata(metadata)
                    .build();
        }
        catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);

            return null;
        }
    }

//  public static String toElasticIndex(DmsEventLoc event) throws JsonProcessingException {
//    return mapper.writeValueAsString(event.get("data"));
//  }

    @Override
    public boolean isEndOfStream(DmsEventLoc DmsEventLoc) {
        return false;
    }

    @Override
    public TypeInformation<DmsEventLoc> getProducedType() {
        return new AvroTypeInfo<>(DmsEventLoc.class);
    }
}
