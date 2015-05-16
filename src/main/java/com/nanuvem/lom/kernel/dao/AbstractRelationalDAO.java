package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractRelationalDAO {

	protected MySqlConnector connector;
	private Connection connection;

	public AbstractRelationalDAO(MySqlConnector connectionFactory) {
		this.connector = connectionFactory;
	}

	protected Connection createConnection() throws SQLException {
		if (this.connection == null || this.connection.isClosed()) {
			this.connection = this.connector.createConnection();
		}
		return connection;
	}

	protected void closeConexao() throws SQLException {
		if (this.connection != null && !this.connection.isClosed()) {
			this.connection.close();
		}
	}

	protected String getDatabaseName() {
		return this.connector.getDatabaseName();
	}
}
