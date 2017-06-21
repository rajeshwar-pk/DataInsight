package com.prokarma.data.cleansing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public enum DataManipulator {
	INSTANCE;
	
	public Dataset<Row> applyFilter(final Dataset<Row> inputData,final FilterCondition... condition){
		Dataset<Row> filteredData = inputData;
		for(FilterCondition cond : condition){
			Dataset<Row> tempData = null;
			tempData = filteredData.filter(cond.getConstraint());
			filteredData = tempData;
		}
		return filteredData;
	}
	
	public Dataset<Row> transformCols(final Dataset<Row> inputData, final TransformCondition... condition) {
		Dataset<Row> formattedData = inputData;
		for (TransformCondition trans : condition) {
			Dataset<Row> tempData = null;
			if (trans.getConvertType() == null) {
				tempData = formattedData.withColumn(trans.getModifiedCol(), formattedData.col(trans.getCol()));
			} else {
				tempData = formattedData.withColumn(trans.getModifiedCol(),
						formattedData.col(trans.getCol()).cast(trans.getConvertType()));
			}
			formattedData = tempData;
		}
		return formattedData;
	}
	
	public Dataset<Row> removeAllNullValues(final Dataset<Row> input){
		return input.na().drop();
	}
	
	public Dataset<Row> removeNullValuesForCols(final Dataset<Row> input,String... cols){
		return input.na().drop(cols);
	}
}
