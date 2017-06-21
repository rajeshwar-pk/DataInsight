package com.prokarma.ai.dataInsight;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

import com.prokarma.ai.data.exception.DataloadException;
import com.prokarma.ai.data.ingest.DataLoader;
import com.prokarma.ai.server.SparkAppStarter;
import com.prokarma.data.cleansing.DataManipulator;
import com.prokarma.data.cleansing.FilterCondition;
import com.prokarma.data.cleansing.TransformCondition;

import junit.framework.TestCase;







public class DataLoaderTest extends TestCase {
		
	@Test
	public void testDataload() {
		SparkSession session = (new SparkAppStarter()).getLocalAppSession("Test");
		Dataset<Row> loadedData = DataLoader.INSTANCE.loadDatafile(session,
				"/home/rajeshwar/Hospitals/CareEffectivenessPrediction");
        Assert.assertNotNull(loadedData);
        loadedData.show();
        session.stop();
	}
	@Test
	public void testLoadModel() {
		SparkSession session = (new SparkAppStarter()).getLocalAppSession("Test");
		PipelineModel model = DataLoader.INSTANCE.loadModel("/home/rajeshwar/Hospitals/CareEffectivenessALSModel");
		Assert.assertNotNull(model);
		System.out.println(model.uid());
		try {
			Dataset<Row> data = DataLoader.INSTANCE.readDatafile(session, true,
					"/home/rajeshwar/Hospitals/Timely_and_Effective_Care_Hospital.csv");
			Assert.assertNotNull(data);
			long initialCount = data.count();
			
			FilterCondition[] filters = getFilterConditions();
			Dataset<Row> dataCleaned = DataManipulator.INSTANCE.applyFilter(data.select("Hospital Name", "Measure Name", "Condition", "Score"),filters);
			long filteredCount = dataCleaned.count();
			TransformCondition[] transforms = getTranforms();
			Dataset<Row> dataFormatted = DataManipulator.INSTANCE.transformCols(dataCleaned, transforms);/*dataCleaned.withColumn("HospitalName", dataCleaned.col("Hospital Name")).
	    			withColumn("MeasureName", dataCleaned.col("Measure Name")).
	    			withColumn("Condition", dataCleaned.col("Condition")).
	    			withColumn("Score", dataCleaned.col("Score").cast(DataTypes.DoubleType));*/
			model.transform(dataFormatted).show();
			System.out.println("Initial Size : " + initialCount);
			System.out.println("Cleaned  Size : " + filteredCount);

		} catch (DataloadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}

		session.stop();
	}

	private TransformCondition[] getTranforms() {
		List<TransformCondition> transforms = new ArrayList<>();
		transforms.add(new TransformCondition("Hospital Name","HospitalName"));
		transforms.add(new TransformCondition("Measure Name","MeasureName"));
		transforms.add(new TransformCondition("Score","Score", DataTypes.DoubleType));
		TransformCondition[] filterArr  = new TransformCondition[transforms.size()];
		transforms.toArray(filterArr);
		return filterArr;
	}

	private FilterCondition[] getFilterConditions() {
		List<FilterCondition> filters = new ArrayList<>();
		filters.add(new FilterCondition("Score","!= 'Not Available'"));
		filters.add(new FilterCondition("Score","not like '%(%'"));
		filters.add(new FilterCondition("Score","not like '%-%'"));
		FilterCondition[] filterArr  = new FilterCondition[filters.size()];
		filters.toArray(filterArr);
		return filterArr;
	}
	
}
