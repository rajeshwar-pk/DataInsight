package com.prokarma.ai.server;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;

public class SparkAppStarter implements SessionProvider {

	private static final String DEFAULT_APP = " DEFAULT APP";

	private static final String LOCAL_SERVER = "local[*]";

	public SparkSession getLocalAppSession(String applicationName) {
		final String appName = StringUtils.isEmpty(applicationName) ? "DEFAULT APP" : applicationName;
		return SparkSession.builder().appName(appName).master(LOCAL_SERVER).getOrCreate();
	}

	public SparkSession getServerAppSession(String applicationName, String serverURL) {
		final String appName = StringUtils.isEmpty(applicationName) ? DEFAULT_APP : applicationName;
		final String server = StringUtils.isEmpty(serverURL) ? LOCAL_SERVER : serverURL;
		return SparkSession.builder().appName(appName).master(LOCAL_SERVER).getOrCreate();
	}

}
