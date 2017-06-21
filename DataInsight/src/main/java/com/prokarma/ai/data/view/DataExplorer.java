package com.prokarma.ai.data.view;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

public enum DataExplorer {
	INSTANCE;

	public String getNRowsAsJson(final Dataset<Row> input, final int rows) {
		String data = null;
		List<String> collectedRows = input.limit(rows).toJSON().collectAsList();
		Gson gson = new Gson();
		data = gson.toJson(collectedRows);
		return data;
	}

	public String getNRowsAsXml(final Dataset<Row> input, final int rows) {
		StringBuilder data = new StringBuilder("<?xml version = \"1.0\" encoding = \"utf-8\"?>\n\t<data>\n");
		List<Row> collectedRows = input.limit(rows).collectAsList();
        for(Row row: collectedRows){
        	int size = row.size();
        	data.append("\t\t<row>\n");
        	for(int i = 0 ; i < size ; i++){
        		data.append("\t\t\t<c_" +i + ">");
        		data.append(row.getString(i));
        		data.append("</c_" +i + ">\n");
        	}
        	data.append("\t\t</row>\n");
        }
		data.append("</data>");
		return data.toString();
	}

	public String getColumnNames(final Dataset<Row> input) {
		StringBuilder colNames = new StringBuilder();
		String[] cols = input.columns();
		for (String col : cols) {
			colNames.append(col).append(",");
		}
		colNames.deleteCharAt(colNames.length() - 1);
		return colNames.toString();
	}
	public long getNumOfNullValues(final Dataset<Row> input, final String col) {
		return input.na().drop(col).count();
	}

	public long getTotalNullValues(final Dataset<Row> input, final String col) {
		return input.na().drop().count();
	}

	public long getNumOfDuplicates(final Dataset<Row> input) {
		return input.dropDuplicates().count();
	}
}
