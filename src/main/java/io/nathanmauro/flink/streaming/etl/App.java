package io.nathanmauro.flink.streaming.etl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.nathanmauro.flink.streaming.etl.events.DmsEventLoc;
import io.nathanmauro.flink.streaming.etl.utils.DmsEventLocSchema;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class App {

    public static void main(String[] args) throws IOException {
        byte[] bytes = App.getRecord().getBytes(StandardCharsets.UTF_8);
        DmsEventLoc event = new DmsEventLocSchema().deserialize(bytes);
//        System.out.println(event.getData());
//
//        XContentBuilder xContent = DmsEventLocSchema.getXContentJson(event);
//        XContentBuilder xContent = DmsEventLocSchema.getXContentJson(event);
        XContentBuilder xContent = DmsEventLocSchema.getXContentJson(event);
        System.out.println(Strings.toString(xContent));
//        ZoneId est = ZoneId.of("-05:00");
//        System.out.println(est.getId());
//        String id = getId(event);
//            HexBinary.encode(dig)
//        System.out.println(id);
//        System.out.println(DmsEventSchema.toElasticIndex(event));
//        System.out.println(DmsEventSchema.toElasticIndexBySCN(event));
    }

    public static String getRecord2() {
        return "{\n\t\"metadata\":\t{\n\t\t\"timestamp\":\t\"2022-01-01T00:00:00.000000Z\",\n\t\t\"record-type\":\t\"control\",\n\t\t\"operation\":\t\"drop-table\",\n\t\t\"partition-key-type\":\t\"task-id\",\n\t\t\"schema-name\":\t\"PLACEHOLDER_SCHEMA\",\n\t\t\"table-name\":\t\"PLACEHOLDER_TABLE\"\n\t}\n}";
    }

    public static String getRecordException() {
        return "{\n\t\"metadata\":\t{\n\t\t\"timestamp\":\t\"2022-03-09T21:19:37.194971Z\",\n\t\t\"record-type\":\t\"control\",\n\t\t\"operation\":\t\"create-table\",\n\t\t\"partition-key-type\":\t\"task-id\",\n\t\t\"schema-name\":\t\"\",\n\t\t\"table-name\":\t\"awsdms_apply_exceptions\"\n\t}\n}";
    }

    public static String getRecord() {
        return "PLACEHOLDER_BUSINESS_ENTITY_ROW";
    }
}
