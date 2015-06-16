package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.nanuvem.lom.api.Entity;

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
