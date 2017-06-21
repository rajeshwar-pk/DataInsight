package com.prokarma.data.cleansing;

import org.apache.spark.sql.types.DataType;

public final class TransformCondition {
	
	private String col;
	private String modifiedCol;
	private DataType convertType;
	
	public TransformCondition(String col, String modifiedCol) {
		super();
		this.col = col;
		this.modifiedCol = modifiedCol;
	}
	
	public TransformCondition(String col, String modifiedCol, DataType convertType) {
		super();
		this.col = col;
		this.modifiedCol = modifiedCol;
		this.convertType = convertType;
	}

	public String getCol() {
		return col;
	}
	
	public String getModifiedCol() {
		return modifiedCol;
	}
	
	public DataType getConvertType() {
		return convertType;
	}
	

}
