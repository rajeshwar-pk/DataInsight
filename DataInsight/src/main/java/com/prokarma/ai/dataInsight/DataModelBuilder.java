package com.prokarma.ai.dataInsight;

import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.dmg.pmml.DataType;
/*
 * UNDER CONSTRUCTION
 */
public class DataModelBuilder {
	
	public PipelineModel createPipelineModel(final Dataset<Row> input, final Pipeline pipeline){
		PipelineModel model = pipeline.fit(input);
		return model;
	}
	
	public VectorAssembler buildFeatures(final List<DataColumn> cols){
		String[] featureArr = new String[cols.size()];
		for(DataColumn col: cols){
			switch(col.getType().simpleString()){
			
			
		    default:
				
			}
		}
		 
		VectorAssembler features = (new VectorAssembler()).setInputCols(featureArr).
	        		setOutputCol("features");
	     
		 return features;
    }

}
