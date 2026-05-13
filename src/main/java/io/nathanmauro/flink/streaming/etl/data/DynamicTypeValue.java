package io.nathanmauro.flink.streaming.etl.data;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public interface DynamicTypeValue {
	Object data();
}
