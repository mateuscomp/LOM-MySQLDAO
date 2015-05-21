package com.nanuvem.lom.kernel.dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MySqlConnector {

	private static final String ADDRESS_PERSISTENCE_FILE = "resources/mypersistence.properties";

	private static final String DATABASE_NAME_PROPERTY_NAME = "databasename";
	private static final String DATABASE_PASSWORD_PROPERTY_NAME = "password";
	private static final String DATABASE_USER_PROPERTY_NAME = "user";
	private static final String DATABASE_PORT_PROPERTY_NAME = "port";
	private static final String DATABASE_ADDRESS_PROPERTY_NAME = "urlconnection";

	private String databaseAddress;
	private String port;
	private String user;
	private String password;
	private String databaseName;

	public MySqlConnector() throws FileNotFoundException, IOException {
		Properties properties = loadProperties();

		this.databaseAddress = properties
				.getProperty(DATABASE_ADDRESS_PROPERTY_NAME);
		this.port = properties.getProperty(DATABASE_PORT_PROPERTY_NAME);
		this.user = properties.getProperty(DATABASE_USER_PROPERTY_NAME);
		this.password = properties.getProperty(DATABASE_PASSWORD_PROPERTY_NAME);
		this.databaseName = properties.getProperty(DATABASE_NAME_PROPERTY_NAME);
	}

	private Properties loadProperties() throws FileNotFoundException,
			IOException {

		Properties myProperties = new Properties();
		// myProperties.load(new FileInputStream(ADDRESS_PERSISTENCE_FILE));
		myProperties.setProperty(DATABASE_ADDRESS_PROPERTY_NAME, "localhost");
		myProperties.setProperty(DATABASE_PORT_PROPERTY_NAME, "3306");
		myProperties.setProperty(DATABASE_USER_PROPERTY_NAME, "root");
		myProperties.setProperty(DATABASE_PASSWORD_PROPERTY_NAME, "root");
		myProperties.setProperty(DATABASE_NAME_PROPERTY_NAME, "lom");
		return myProperties;
	}

	public Connection createConnection() throws SQLException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			return DriverManager.getConnection(this.getUrl(), this.user,
					this.password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String getDatabaseName() {
		return this.databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	private String getUrl() {
		String url = "jdbc:mysql://" + databaseAddress + ":" + port;

		if (databaseName != null && !databaseName.isEmpty()) {
			url += "/" + databaseName;
		}

		return url;
	}
}
