package io.nathanmauro.flink.streaming.etl.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.data.TimeConversions;
import org.elasticsearch.common.geo.GeoPoint;

import java.beans.Transient;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class DmsEventType {
	@JsonProperty("metadata")
	private Metadata metadata;

	@JsonProperty("data")
	private Map<String, Object> data;

	@JsonProperty("location")
	GeoPoint location;

	public void setMetadata(Metadata metadata){
		this.metadata = metadata;
	}

	public Metadata getMetadata(){
		return metadata;
	}

	public void setData(Map<String, Object> data){
		this.data = data;
	}

	public Map<String, Object> getData(){
		return data;
	}
	public GeoPoint getLocation() {
		return location;
	}

	public void setLocation(GeoPoint location) {
		this.location = location;
	}
}
