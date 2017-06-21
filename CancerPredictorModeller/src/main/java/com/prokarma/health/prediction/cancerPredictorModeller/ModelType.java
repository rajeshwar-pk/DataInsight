package com.prokarma.health.prediction.cancerPredictorModeller;

public enum ModelType {
 DTM("Decision Tree Modeller"),GBM("Gradient Boosted Modeller");
 private String modelName;
	
 private ModelType(final String name){
	 this.modelName = name;
 }
 
 public String getName(){
	 return this.modelName;
 }
}
