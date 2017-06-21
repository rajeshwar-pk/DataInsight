package com.prokarma.health.prediction.cancerPredictorModeller;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CompoundModel {
	
	private double accuracy;
	
	private double rmse;
	
	private Dataset<Row> prediction;
	
	private PipelineModel model;
	
	private ConfusionMatrix cmatrix;

	
	public ConfusionMatrix getCmatrix() {
		return cmatrix;
	}

	public void setCmatrix(ConfusionMatrix cmatrix) {
		this.cmatrix = cmatrix;
	}

	public double getAccuracy() {
		return accuracy;
	}

	public void setAccuracy(double accuracy) {
		this.accuracy = accuracy;
	}

	public double getRmse() {
		return rmse;
	}

	public void setRmse(double rmse) {
		this.rmse = rmse;
	}

	public Dataset<Row> getPrediction() {
		return prediction;
	}

	public void setPrediction(Dataset<Row> prediction) {
		this.prediction = prediction;
	}

	public PipelineModel getModel() {
		return model;
	}

	public void setModel(PipelineModel model) {
		this.model = model;
	}

}
