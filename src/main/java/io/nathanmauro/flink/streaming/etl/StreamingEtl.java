/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file has been extended from the Apache Flink project skeleton.
 *
 */

package io.nathanmauro.flink.streaming.etl;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kinesis.shaded.com.amazonaws.regions.Regions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nathanmauro.flink.streaming.etl.events.DmsEventLoc;
import io.nathanmauro.flink.streaming.etl.utils.AmazonElasticsearchSink;
import io.nathanmauro.flink.streaming.etl.utils.DmsEventLocSchema;
import io.nathanmauro.flink.streaming.etl.utils.ParameterToolUtils;
import com.fasterxml.jackson.core.JsonProcessingException;

public class StreamingEtl {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingEtl.class);

    private static final String DEFAULT_REGION_NAME;

    static {
        String regionName = "us-east-1";

        try {
            regionName = Regions.getCurrentRegion().getName();
        }
        catch (Exception ignored) {
        }
        finally {
            DEFAULT_REGION_NAME = regionName;
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> events;

        if (parameter.has("InputKinesisStream")
                && (parameter.has("InputKafkaBootstrapServers") || parameter.has("InputKafkaTopic"))) {
            throw new RuntimeException(
                    "You can only specify a single source, either a Kinesis data stream or a Kafka topic");
        }
        else if (parameter.has("InputKinesisStream")) {
            LOG.info("Reading from {} Kinesis stream", parameter.get("InputKinesisStream"));

            events = env.addSource(getKinesisSource(parameter)).name("Kinesis source");
        }
        else {
            throw new RuntimeException(
                    "Missing runtime parameters: Specify 'InputKinesisStreamName' xor ('InputKafkaBootstrapServers' and 'InputKafkaTopic') as a parameters to the Flink job");
        }

        SingleOutputStreamOperator<DmsEventLoc> dmsEvents = events
                .map(ev -> new DmsEventLocSchema().deserialize(ev.getBytes(StandardCharsets.UTF_8)));

//        dmsEvents.print();

        KeyedStream<DmsEventLoc, CharSequence> keyedDmsEvents = dmsEvents
                .filter(Objects::nonNull)
                .filter(ev -> ev.getMetadata() != null && ev.getMetadata().get("record-type") != null)
                .filter(ev -> {
                    LOG.info("Event Metadata: " + ev.getMetadata().toString());
                    String recordType = ev.getMetadata().get("record-type").toString();

                    return recordType.equals("data");
                })
                .keyBy(ev -> {
                    String tableName = ev.getMetadata().get("table-name").toString();
                    String recordType = ev.getMetadata().get("record-type").toString();
                    String operation = ev.getMetadata().get("operation").toString();
                    String schemaName = ev.getMetadata().get("schema-name").toString();
                    LOG.info("Event: " + schemaName + "." + tableName + "|" + operation + "|" + recordType + "\n" +
                            "Event Data: " + ev.getData());
                    return ev.getMetadata().get("table-name");
                });

//        keyedDmsEvents.print();

//        if (parameter.has("OutputBucket")) {
//
//            LOG.info("Writing to {} buket", parameter.get("OutputBucket"));
//
//            keyedDmsEvents.addSink(getS3Sink(parameter)).name("S3 Sink");
//        }

        // region Elastic Search Output
        if (parameter.has("OutputElasticsearchEndpoint")) {
            LOG.info("Writing to {} ES endpoint", parameter.has("OutputElasticsearchEndpoint"));

            keyedDmsEvents.addSink(getElasticsearchSink(parameter)).name("Elastic Search Sink");
        }
        // endregion Elastic Search Output

        // region output stream
        if (parameter.has("OutputKinesisStream")) {
            LOG.info("Writing to {} Kinesis stream", parameter.get("OutputKinesisStream"));

//            events
//                    .keyBy(ev -> ev.getMetadata().getTableName().toString())
//                    .addSink(getKinesisSink(parameter))
//                    .name("Kinesis sink");
        }
        // endregion output stream

        // region discarding
        if (parameter.has("OutputDiscarding")) {
            LOG.info("Writing to Discarding sink");

            events
                    .addSink(new DiscardingSink<>())
                    .name("Discarding sink");
        }

        if (!(parameter.has("OutputDiscarding") || parameter.has("OutputBucket")
                || parameter.has("OutputElasticsearchEndpoint")
                || (parameter.has("OutputKafkaBootstrapServers") && parameter.has("OutputKafkaTopic"))
                || parameter.has("OutputKinesisStream"))) {
            throw new RuntimeException(
                    "Missing runtime parameters: Specify 'OutputDiscarding' or 'OutputBucket' or 'OutputElasticsearchEndpoint' or ('OutputKafkaBootstrapServers' and 'OutputKafkaTopic') as a parameters to the Flink job");
        }
        // endregion discarding

        env.execute();
    }

    private static SourceFunction<String> getKinesisSource(ParameterTool parameter) {
        String streamName = parameter.getRequired("InputKinesisStream");
        String region = parameter.get("InputStreamRegion", DEFAULT_REGION_NAME);
        String initialPosition = parameter.get("InputStreamInitalPosition",
                ConsumerConfigConstants.DEFAULT_STREAM_INITIAL_POSITION);
        String timestamp = parameter.get("TimestampPosition", ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP);

        // set Kinesis consumer properties
        Properties kinesisConsumerConfig = new Properties();
        // set the region the Kinesis stream is located in
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, region);
        // obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadataConverter
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        // poll new events from the Kinesis stream once every second
//		kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");

        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, initialPosition);

        if (initialPosition.equalsIgnoreCase("AT_TIMESTAMP")) {
            kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP,
                    parameter.get("TimestampPosition"));
        }

        return new FlinkKinesisConsumer<>(streamName, new SimpleStringSchema(), kinesisConsumerConfig);
    }

    private static SinkFunction<DmsEventLoc> getS3Sink(ParameterTool parameter) {
        String bucket = parameter.getRequired("OutputBucket");
        String prefix = String.format("%sjob_start=%s/", parameter.get("OutputPrefix", ""), System.currentTimeMillis());

        String dateTimeFormat = "'year='yyyy'/month='MM'/day='dd'/hour='HH/";
        return StreamingFileSink
                .forRowFormat(
                        new Path(bucket),
                        (Encoder<DmsEventLoc>) (element, outputStream) -> {
                            PrintStream out = new PrintStream(outputStream);
                            out.println(DmsEventLocSchema.toJson(element));
                        })
                .withBucketAssigner(new DateTimeBucketAssigner<>(dateTimeFormat, ZoneId.of("-05:00")))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build();
    }

    private static SinkFunction<DmsEventLoc> getElasticsearchSink(ParameterTool parameter) {
        String elasticsearchEndpoint = parameter.getRequired("OutputElasticsearchEndpoint");
        String region = parameter.get("ElasticsearchRegion", DEFAULT_REGION_NAME);

        ElasticsearchSink.Builder<DmsEventLoc> builder = AmazonElasticsearchSink.elasticsearchSinkBuilder(
                elasticsearchEndpoint,
                region,
                new ElasticsearchSinkFunction<DmsEventLoc>() {
                    IndexRequest createIndexRequest(DmsEventLoc element) throws IOException {
                        String type = element.getMetadata().get("table-name").toString();
                        String id = getId(element);

                        XContentBuilder json = null;
                        try {
                            json = DmsEventLocSchema.getXContentJson(element);
                            LOG.info("Elastic JSON: " + json);
                        }
                        catch (JsonProcessingException e) {
                            e.printStackTrace();
                            LOG.error("Error Converting: " + e.getMessage());
                            LOG.error("Error Converting: " + element.getData().toString());
                        }


                        IndexRequest indexRequest = Requests.indexRequest();

                        return indexRequest.create(false)
                                .index("dms_flink_" + type.toLowerCase())
                                //                                .type("_doc")
                                .id(id)
                                .source(json);
                    }

                    @Override
                    public void process(DmsEventLoc element, RuntimeContext ctx, RequestIndexer indexer) {
                        try {
                            indexer.add(createIndexRequest(element));
                        } catch (IOException e) {
                            e.printStackTrace();
                            LOG.error("Error Indexing: " + e.getMessage());
                        }
                    }
                });

        return getElasticEventSinkFunction(parameter, builder);
    }

    private static SinkFunction<DmsEventLoc> getElasticEventSinkFunction(ParameterTool parameter,
                                                                      ElasticsearchSink.Builder<DmsEventLoc> builder) {
//        builder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        builder.setFailureHandler(new NoOpFailureHandler());

        if (parameter.has("ElasticsearchBulkFlushMaxSizeMb")) {
            builder.setBulkFlushMaxSizeMb(parameter.getInt("ElasticsearchBulkFlushMaxSizeMb"));
        }

        if (parameter.has("ElasticsearchBulkFlushMaxActions")) {
            builder.setBulkFlushMaxActions(parameter.getInt("ElasticsearchBulkFlushMaxActions"));
        }

        if (parameter.has("ElasticsearchBulkFlushInterval")) {
            builder.setBulkFlushInterval(parameter.getLong("ElasticsearchBulkFlushInterval"));
        }

        return builder.build();
    }

    private static SinkFunction<DmsEventLoc> getKinesisSink(ParameterTool parameter) {
        String streamName = parameter.getRequired("OutputKinesisStream");
        String region = parameter.get("OutputStreamRegion", DEFAULT_REGION_NAME);

        Properties properties = new Properties();
        properties.setProperty(AWSConfigConstants.AWS_REGION, region);
        properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

        FlinkKinesisProducer<DmsEventLoc> producer = new FlinkKinesisProducer<>(new DmsEventLocSchema(), properties);
        producer.setFailOnError(true);
        producer.setDefaultStream(streamName);
        producer.setDefaultPartition("0");

        return producer;
    }

    public static String getId(DmsEventLoc event) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] dataBytes = md.digest(event.getData().toString().getBytes(StandardCharsets.UTF_8));
            byte[] dig = md.digest(dataBytes);
            StringBuilder hexStringBuffer = new StringBuilder();
            for (byte b : dig) {
                char[] hexDigits = new char[2];
                hexDigits[0] = Character.forDigit((b >> 4) & 0xF, 16);
                hexDigits[1] = Character.forDigit((b & 0xF), 16);
                String hexString = new String(hexDigits);
                hexStringBuffer.append(hexString);
            }
            String id = hexStringBuffer.toString();
            return id;
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }
}
