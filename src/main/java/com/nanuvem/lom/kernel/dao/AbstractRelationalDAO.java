package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractRelationalDAO {

	private MySqlConnectionFactory connectionFactory;
	private Connection connection;

	public AbstractRelationalDAO(MySqlConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	protected Connection criarConexao() throws SQLException {
		if (this.connection == null || this.connection.isClosed()) {
			this.connection = this.connectionFactory.criarConexao();
		}
		return connection;
	}

	protected void fecharConexao() throws SQLException {
		if (this.connection != null && !this.connection.isClosed()) {
			this.connection.close();
		}
	}

	protected String getDatabaseName() {
		return this.connectionFactory.getDatabaseName();
	}

	public abstract String getNameTable();
}
