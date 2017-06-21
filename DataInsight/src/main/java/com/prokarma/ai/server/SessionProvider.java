package com.prokarma.ai.server;

import org.apache.spark.sql.SparkSession;

/**
 * Interface for initialization of
 * 
 * @author rajeshwar
 *
 */
public interface SessionProvider {
	/**
	 * This method runs app on a local spark instance and returns the Spark
	 * Session object.
	 * 
	 * @param applicationName
	 * @return
	 */
	SparkSession getLocalAppSession(String applicationName);

	/**
	 * This method runs app on a specified server spark instance and returns the
	 * Spark Session object.
	 * 
	 * @param applicationName
	 * @param serverURL
	 * @return
	 */
	SparkSession getServerAppSession(String applicationName, String serverURL);

}
