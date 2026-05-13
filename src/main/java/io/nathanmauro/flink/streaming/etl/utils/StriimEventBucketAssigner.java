///*
// * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// *
// * Permission is hereby granted, free of charge, to any person obtaining a copy of this
// * software and associated documentation files (the "Software"), to deal in the Software
// * without restriction, including without limitation the rights to use, copy, modify,
// * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
// * permit persons to whom the Software is furnished to do so.
// *
// * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
// * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// */
//
//package io.nathanmauro.flink.streaming.etl.utils;
//
//import java.io.Serializable;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.time.Instant;
//import java.util.Date;
//
//import io.nathanmauro.flink.streaming.etl.data.StriimEventType;
//import org.apache.flink.core.io.SimpleVersionedSerializer;
//import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
//import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
//
//public class StriimEventBucketAssigner implements BucketAssigner<StriimEventType, String>, Serializable {
//    private final String prefix;
//
//    public StriimEventBucketAssigner(String prefix) {
//        this.prefix = prefix;
//    }
//
//    public String getBucketId(StriimEventType event, Context context) {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date date = null;
//        try {
//            date = format.parse(String.valueOf(Date.from(Instant.ofEpochSecond(event.getMetadata().getDBTimeStamp()))));
//        }
//        catch (ParseException e) {
//            e.printStackTrace();
//        }
//        SimpleDateFormat dfy = new SimpleDateFormat("yyyy");
//        SimpleDateFormat dfm = new SimpleDateFormat("MM");
//        String year = dfy.format(date);
//        String month = dfm.format(date);
//
//        return String.format("%stable_name=%s/year=%s/month=%s",
//                prefix,
//                event.getMetadata().getTableName(),
//                year,
//                month
////        event.getPickupDatetime().getMonthOfYear()
//        );
//    }
//
//    public SimpleVersionedSerializer<String> getSerializer() {
//        return SimpleVersionedStringSerializer.INSTANCE;
//    }
//}
