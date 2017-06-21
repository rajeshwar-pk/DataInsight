package com.prokarma.health.prediction.cancerPredictorModeller;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.prokarma.ai.data.exception.DataloadException;
import com.prokarma.ai.data.ingest.DataLoader;
import com.prokarma.ai.server.SparkAppStarter;

/**
 * Hello world!
 *
 */
public class BreastCancerPredictorApp {
	public static void main(String[] args) {
		SparkSession session = (new SparkAppStarter()).getLocalAppSession("Breast Cancer Predictor App");
		try {
			CancerDataModeller modeller = new CancerDataModeller();
		/*
		CompoundModel model = modeller.createMultiLayerPerceptronModel(session,new String[]{"/home/rajeshwar/Hospitals/cancer_data/Breast_cancer_data.csv"},64,5L);
		System.out.println("accuracy "+ model.getAccuracy());
		*/
		/*CompoundModel model = modeller.createGradientBoostedModel(session, new String[]{"/home/rajeshwar/Hospitals/cancer_data/Breast_cancer_data.csv"},10);
		System.out.println("rmse "+ model.getRmse());*/
		
			Dataset<Row> data = DataLoader.INSTANCE.readDatafile(session, true, "/home/rajeshwar/Downloads/risk_factors_cervical_cancer.csv");
			//data.show(false);
			CompoundModel model = modeller.createDecisionTreeModel(session,new String[]{"/home/rajeshwar/Hospitals/cancer_data/Breast_cancer_data.csv"});
		
		
		System.out.println("Positive Positive : " + model.getCmatrix().getPosPos() + "\n");
		System.out.println("Positive Negative : " + model.getCmatrix().getPosNeg() + "\n");
		System.out.println("Negative Negative : " + model.getCmatrix().getNegNeg() + "\n");
		System.out.println("Negative Positive : " + model.getCmatrix().getNegPos() + "\n");
		System.out.println("accuracy "+ model.getAccuracy());
		//call GBM
		} catch (DataloadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		session.stop();
	}
}
