package com.prokarma.ai.dataInsight;

import static org.junit.Assert.fail;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.prokarma.ai.data.exception.DataloadException;
import com.prokarma.ai.data.ingest.DataLoader;
import com.prokarma.ai.data.view.DataExplorer;
import com.prokarma.ai.server.SparkAppStarter;

public class DataExplorerTest {
	
	private SparkSession session;

	@Before
	public void setUp() throws Exception {
		session = (new SparkAppStarter()).getLocalAppSession("Test");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetNRowsAsJson() {
		Dataset<Row> data = session.read().csv("/home/rajeshwar/Hospitals/Timely_and_Effective_Care_Hospital.csv");
		String first50 = DataExplorer.INSTANCE.getNRowsAsJson(data, 50);
		Assert.assertNotNull(first50);
		System.out.println(first50);
	}
	
	@Test
	public void testGetNRowsAsXml() {
		Dataset<Row> data = session.read().csv("/home/rajeshwar/Hospitals/READMISSION_REDUCTION.csv");
		String first50 = DataExplorer.INSTANCE.getNRowsAsXml(data, 50);
		Assert.assertNotNull(first50);
		System.out.println(first50);
	}
	
	@Test
	public void testHeader() throws DataloadException{
		Dataset<Row> data = DataLoader.INSTANCE.readDatafile(session, true,	"/home/rajeshwar/Hospitals/READMISSION_REDUCTION.csv");
		String colNames = DataExplorer.INSTANCE.getColumnNames(data);
		Assert.assertNotNull(colNames);
		System.out.println(colNames);
	}

}
