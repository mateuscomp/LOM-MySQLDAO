package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySqlConnectionFactory {

	private String databaseAddress;
	private String port;
	private String user;
	private String password;
	private String databaseName;

	public MySqlConnectionFactory(String databaseAddress, String port,
			String user, String pasword) {

		this.databaseAddress = databaseAddress;
		this.port = port;
		this.user = user;
		this.password = pasword;
	}

	public Connection criarConexao() throws SQLException {
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
		return "jdbc:mysql://" + databaseAddress + ":" + port;
	}
}
