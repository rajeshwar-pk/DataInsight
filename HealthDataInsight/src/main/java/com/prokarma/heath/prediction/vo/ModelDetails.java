package com.prokarma.heath.prediction.vo;

import com.prokarma.health.prediction.cancerPredictorModeller.ConfusionMatrix;

public class ModelDetails {
	
	private String modelId;
	private double rmse;
	private double accuracy;
	private ConfusionMatrix cmatrix;
	
	public ConfusionMatrix getCmatrix() {
		return cmatrix;
	}
	public void setCmatrix(ConfusionMatrix cmatrix) {
		this.cmatrix = cmatrix;
	}
	public String getModelId() {
		return modelId;
	}
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}
	public double getRmse() {
		return rmse;
	}
	public void setRmse(double rmse) {
		this.rmse = rmse;
	}
	public double getAccuracy() {
		return accuracy;
	}
	public void setAccuracy(double accuracy) {
		this.accuracy = accuracy;
	}

}
