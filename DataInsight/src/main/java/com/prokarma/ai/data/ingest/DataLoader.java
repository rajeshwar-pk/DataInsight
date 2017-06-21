package com.prokarma.ai.data.ingest;

import java.io.File;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import com.prokarma.ai.data.exception.DataloadException;

public enum DataLoader {

	INSTANCE;

	
	public Dataset<Row> readDatafile(SparkSession session,boolean header,String filepath) throws DataloadException {
		File file = new File(filepath);
		if(file.isDirectory()){
			throw new DataloadException(filepath + " is a directory");
		}
		String fileType = resolveExtension(filepath);
		Dataset<Row> dataset = null;
		switch (fileType) {
		case "csv":
			if (header) {
				dataset = session.read().option("header", true).csv(filepath);
			} else {
				dataset = session.read().csv(filepath);
			}
			break;
		case "parquet":
			if (header) {
				dataset = session.read().option("header", true).parquet(filepath);
			} else {
				dataset = session.read().parquet(filepath);
			}
			break;
		case "json":
			if (header) {
				dataset = session.read().option("header", true).json(filepath);
			} else {
				dataset = session.read().json(filepath);
			}
			break;
		case "orc":
			if (header) {
				dataset = session.read().option("header", true).orc(filepath);
			} else {
				dataset = session.read().orc(filepath);
			}
			break;
		default:
			if (header) {
				dataset = session.read().option("header", true).text(filepath);
			} else {
				dataset = session.read().text(filepath);
			}
		}
		return dataset;
	}
	
	public Dataset<Row> readDatafile(SparkSession session,  boolean header,String... filepath) throws DataloadException {
		for(String path : filepath){
		File file = new File(path);
		if(file.isDirectory()){
			throw new DataloadException(path + " is a directory");
		}
		}
		String fileType = resolveExtension(filepath[0]);
		Dataset<Row> dataset = null;
		switch (fileType) {
		case "csv":
			if (header) {
				dataset = session.read().option("header", true).csv(filepath);
			} else {
				dataset = session.read().csv(filepath);
			}
			break;
		case "parquet":
			if (header) {
				dataset = session.read().option("header", true).parquet(filepath);
			} else {
				dataset = session.read().parquet(filepath);
			}
			break;
		case "json":
			if (header) {
				dataset = session.read().option("header", true).json(filepath);
			} else {
				dataset = session.read().json(filepath);
			}
			break;
		case "orc":
			if (header) {
				dataset = session.read().option("header", true).orc(filepath);
			} else {
				dataset = session.read().orc(filepath);
			}
			break;
		default:
			if (header) {
				dataset = session.read().option("header", true).text(filepath);
			} else {
				dataset = session.read().text(filepath);
			}
		}
		return dataset;
	} 
    
	private String resolveExtension(String filepath) {
		String[] fileElems = filepath.split("\\.");
		String extension = fileElems[fileElems.length - 1];
		return extension;
	}
	
	public Dataset<Row> loadDatafile(final SparkSession session, final String filepath){
		Dataset<Row> loadedData = session.read().load(filepath);
		return loadedData;
	}
	
	public Dataset<Row> loadDatafile(final SparkSession session, final String... filepath){
		Dataset<Row> loadedData = session.read().load(filepath);
		return loadedData;
	}
	
	public PipelineModel loadModel(final String path){
		PipelineModel model = PipelineModel.read().load(path);
		return model;
	}

}
