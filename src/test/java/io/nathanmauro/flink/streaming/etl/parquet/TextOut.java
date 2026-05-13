package io.nathanmauro.flink.streaming.etl.parquet;

public class TextOut {

    public String data;

    public TextOut(String text) {
        this.data = text;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

}
