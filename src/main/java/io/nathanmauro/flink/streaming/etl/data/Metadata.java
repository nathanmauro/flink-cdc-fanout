package io.nathanmauro.flink.streaming.etl.data;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Metadata{

	@JsonProperty("record-type")
	private String recordType;

	@JsonProperty("schema-name")
	private String schemaName;

	@JsonProperty("partition-key-type")
	private String partitionKeyType;

	@JsonProperty("table-name")
	private String tableName;

	@JsonProperty("operation")
	private String operation;

	@JsonProperty("timestamp")
	private String timestamp;

	public void setRecordType(String recordType){
		this.recordType = recordType;
	}

	public String getRecordType(){
		return recordType;
	}

	public void setSchemaName(String schemaName){
		this.schemaName = schemaName;
	}

	public String getSchemaName(){
		return schemaName;
	}

	public void setPartitionKeyType(String partitionKeyType){
		this.partitionKeyType = partitionKeyType;
	}

	public String getPartitionKeyType(){
		return partitionKeyType;
	}

	public void setTableName(String tableName){
		this.tableName = tableName;
	}

	public String getTableName(){
		return tableName;
	}

	public void setOperation(String operation){
		this.operation = operation;
	}

	public String getOperation(){
		return operation;
	}

	public void setTimestamp(String timestamp){
		this.timestamp = timestamp;
	}

	public String getTimestamp(){
		return timestamp;
	}
}
