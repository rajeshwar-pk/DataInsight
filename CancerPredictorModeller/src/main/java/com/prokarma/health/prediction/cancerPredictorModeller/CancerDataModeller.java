package com.prokarma.health.prediction.cancerPredictorModeller;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.prokarma.ai.data.exception.DataloadException;
import com.prokarma.ai.data.ingest.DataLoader;
import com.prokarma.data.cleansing.DataManipulator;

public class CancerDataModeller {

	public CompoundModel createGradientBoostedModel(SparkSession session, String[] filepath,int maxIter) {
		CompoundModel cmodel = new CompoundModel();
		PipelineModel model = null;
		try {
			Dataset<Row> relData = prepareData(session, filepath);
			Dataset<Row>[] splits = relData.randomSplit(new double[] { 0.7, 0.3 });
			Dataset<Row> trainingData = splits[0];
			Dataset<Row> testData = splits[1];
			StringIndexerModel clsIdx = (new StringIndexer()).setInputCol("DIAGNOSIS").setOutputCol("DiagIdx")
					.fit(relData);
			// Convert indexed labels back to original labels.
			IndexToString labelConverter = (new IndexToString()).setInputCol("prediction")
					.setOutputCol("predictedLabel").setLabels(clsIdx.labels());
			VectorAssembler vector = (new VectorAssembler()).setInputCols(relData.drop("DIAGNOSIS","CaseNo").columns())
					.setOutputCol("Features");
			// Train a GBT model.
		    GBTRegressor gbt = new GBTRegressor()
		      .setLabelCol("DiagIdx")
		      .setFeaturesCol("Features")
		      .setMaxIter(maxIter);

		    // Chain indexer and GBT in a Pipeline.
		    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {clsIdx, vector,gbt,labelConverter});

		    // Train model. This also runs the indexer.
		    model = pipeline.fit(trainingData);

		    // Make predictions.
		    Dataset<Row> predictions = model.transform(testData);
		    
		    RegressionEvaluator evaluator = new RegressionEvaluator()
		      .setLabelCol("DiagIdx")
		      .setPredictionCol("prediction")
		      .setMetricName("rmse");
		    
		    double rmse = evaluator.evaluate(predictions);
		    cmodel.setModel(model);
		    cmodel.setRmse(rmse);
		    predictions.createOrReplaceTempView("PREDICTIONS");
			long pp = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS='car' and predictedLabel='car'").count();
			long pn = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS='car' and predictedLabel!='car'").count();
			long nn = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS!='car' and predictedLabel!='car'").count();
			long np = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS!='car' and predictedLabel='car'").count();
			cmodel.setCmatrix(new ConfusionMatrix(pp, pn, nn, np));
		    
		}catch (DataloadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cmodel;
	}

	public Dataset<Row> prepareData(SparkSession session, String[] filepath) throws DataloadException {
		Dataset<Row> data;
		data = DataLoader.INSTANCE.readDatafile(session, true, filepath);
		Dataset<Row> cleanedData = DataManipulator.INSTANCE.removeAllNullValues(data);
		Dataset<Row> relData = cleanedData.select(cleanedData.col("P").cast(DataTypes.DoubleType),
				cleanedData.col("DR").cast(DataTypes.DoubleType),
				cleanedData.col("MAX IP").cast(DataTypes.DoubleType).as("MaxIP"),
				cleanedData.col("A/DA").cast(DataTypes.DoubleType),
				cleanedData.col("Area").cast(DataTypes.DoubleType),
				cleanedData.col("DA").cast(DataTypes.DoubleType), cleanedData.col("HFS").cast(DataTypes.DoubleType),
				cleanedData.col("PA500").cast(DataTypes.DoubleType),
				cleanedData.col("I0").cast(DataTypes.DoubleType), 
				cleanedData.col("Class").as("DIAGNOSIS"),cleanedData.col("Case #").as("CaseNo"));
		return relData;
	}

	public CompoundModel createDecisionTreeModel(SparkSession session, String[] filepath) {
		CompoundModel cmodel = new CompoundModel();
		PipelineModel model = null;
		try {
			Dataset<Row> relData = prepareData(session, filepath);
			Dataset<Row>[] splits = relData.randomSplit(new double[] { 0.7, 0.3 });
			Dataset<Row> trainingData = splits[0];
			Dataset<Row> testData = splits[1];
			StringIndexerModel clsIdx = (new StringIndexer()).setInputCol("DIAGNOSIS").setOutputCol("DiagIdx")
					.fit(relData);
			// Convert indexed labels back to original labels.
			IndexToString labelConverter = (new IndexToString()).setInputCol("prediction")
					.setOutputCol("predictedLabel").setLabels(clsIdx.labels());
			VectorAssembler vector = (new VectorAssembler()).setInputCols(relData.drop("DIAGNOSIS","CaseNo").columns())
					.setOutputCol("Features");
			DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("DiagIdx").setFeaturesCol("Features");
			Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { clsIdx, vector, dt, labelConverter });
			model = pipeline.fit(trainingData);
			Dataset<Row> predictions = model.transform(testData);
			MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("DiagIdx")
					.setPredictionCol("prediction").setMetricName("accuracy");
			double accuracy = evaluator.evaluate(predictions);
			predictions.createOrReplaceTempView("PREDICTIONS");
			long pp = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS='car' and predictedLabel='car'").count();
			long pn = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS='car' and predictedLabel!='car'").count();
			long nn = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS!='car' and predictedLabel!='car'").count();
			long np = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS!='car' and predictedLabel='car'").count();
			cmodel.setModel(model);
			cmodel.setAccuracy(accuracy);
			cmodel.setCmatrix(new ConfusionMatrix(pp, pn, nn, np));
		} catch (DataloadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cmodel;
	}
	
	public CompoundModel createMultiLayerPerceptronModel(final SparkSession session, final String[] filepath,
			final int blockSize, final long seed) {
		CompoundModel cmodel = new CompoundModel();
		PipelineModel model = null;
		try {
			Dataset<Row> relData = prepareData(session, filepath);
			Dataset<Row>[] splits = relData.randomSplit(new double[] { 0.5, 0.5 });
			Dataset<Row> trainingData = splits[0];
			Dataset<Row> testData = splits[1];
			StringIndexerModel clsIdx = (new StringIndexer()).setInputCol("DIAGNOSIS").setOutputCol("label")
					.fit(relData);
			// Convert indexed labels back to original labels.
			IndexToString labelConverter = (new IndexToString()).setInputCol("prediction")
					.setOutputCol("predictedLabel").setLabels(clsIdx.labels());
			VectorAssembler vector = (new VectorAssembler()).setInputCols(relData.drop("DIAGNOSIS","CaseNo").columns())
					.setOutputCol("features");
			//DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("DiagIdx").setFeaturesCol("Features");
			int[] layers = {9,7,6};
			// create the trainer and set its parameters
			MultilayerPerceptronClassifier mpc = new MultilayerPerceptronClassifier()
			  .setLayers(layers)
			  .setBlockSize(blockSize)
			  .setSeed(seed)
			  .setMaxIter(100);//.setLabelCol("DiagIdx").setFeaturesCol("Features");
			Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { clsIdx, vector, mpc,labelConverter });
			model = pipeline.fit(trainingData);
			Dataset<Row> predictions = model.transform(testData);
			MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("label")
					.setPredictionCol("prediction").setMetricName("accuracy");
			double accuracy = evaluator.evaluate(predictions);
			predictions.createOrReplaceTempView("PREDICTIONS");
			long pp = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS='car' and predictedLabel='car'").count();
			long pn = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS='car' and predictedLabel!='car'").count();
			long nn = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS!='car' and predictedLabel!='car'").count();
			long np = session.sql("select DIAGNOSIS from PREDICTIONS where DIAGNOSIS!='car' and predictedLabel='car'").count();
			cmodel.setModel(model);
			cmodel.setAccuracy(accuracy);
			cmodel.setCmatrix(new ConfusionMatrix(pp, pn, nn, np));
		} catch (DataloadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cmodel;
	}

	public int[] getLayerArray(final int layerNo, final int noOfOutput) {
		int[] layers = new int[layerNo+2];
		layers[layers.length-1] = noOfOutput;
		int i = layers.length -2;
		for(;i > -1 ;i--){
			layers[i]= layers[i+1] +1;
		}
		return layers;
	}

}
