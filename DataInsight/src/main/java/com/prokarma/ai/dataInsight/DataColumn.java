package com.prokarma.ai.dataInsight;

import org.apache.spark.sql.types.DataType;

public final class DataColumn {
	
	private String name;
	
	private DataType type;

	public DataColumn(String name, DataType type) {
		super();
		this.name = name;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public DataType getType() {
		return type;
	}
	
	

}
