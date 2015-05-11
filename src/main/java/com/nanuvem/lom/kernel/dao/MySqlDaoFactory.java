package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.nanuvem.lom.api.dao.AttributeDao;
import com.nanuvem.lom.api.dao.AttributeValueDao;
import com.nanuvem.lom.api.dao.DaoFactory;
import com.nanuvem.lom.api.dao.EntityDao;
import com.nanuvem.lom.api.dao.InstanceDao;

public class MySqlDaoFactory implements DaoFactory {

	private MySqlConnectionFactory connectionFactory;

	private MySqlEntityDao entityDao;
	private MySqlAttributeDao attributeDao;
	private MySqlInstanceDao instanceDao;
	private MySqlAttributeValueDao attributeValueDao;

	public MySqlDaoFactory() {
		this.connectionFactory = new MySqlConnectionFactory("localhost",
				"3306", "root", "root");
	}

	public EntityDao createEntityDao() {
		if (this.entityDao == null) {
			this.entityDao = new MySqlEntityDao(this.connectionFactory);
		}
		return this.entityDao;
	}

	public AttributeDao createAttributeDao() {
		if (attributeDao == null) {
			this.attributeDao = new MySqlAttributeDao(this.connectionFactory,
					this.createEntityDao());
		}
		return this.attributeDao;
	}

	public InstanceDao createInstanceDao() {
		if (this.instanceDao == null) {
			this.instanceDao = new MySqlInstanceDao(this.connectionFactory,
					this.createEntityDao());
		}
		return this.instanceDao;
	}

	public AttributeValueDao createAttributeValueDao() {
		if (this.attributeValueDao == null) {
			this.attributeValueDao = new MySqlAttributeValueDao(
					this.connectionFactory);
		}
		return this.attributeValueDao;
	}

	public void createDatabaseSchema() {
		String createDatabaseCommand = "CREATE DATABASE lom";
		String createEntityTypeTable = "CREATE TABLE `lom`.`entityType` ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
				+ "`version` int(11) NOT NULL DEFAULT '0', "
				+ "`namespace` varchar(45) NOT NULL, "
				+ "`name` varchar(45) DEFAULT NULL, " + "PRIMARY KEY (`id`))";

		// String createPropertyTypeTable =
		// "CREATE TABLE `propertyType` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `version` int(11) NOT NULL DEFAULT '0', `sequence` varchar(45) DEFAULT NULL, `name` varchar(45) NOT NULL, `configuration` longtext, `entityType_id` bigint(20) NOT NULL, `type` varchar(45) NOT NULL, PRIMARY KEY `id`), KEY `fk_attributeType_entityType_idx` (`entityType_id`), CONSTRAINT `fk_attributeType_entityType` FOREIGN KEY (`entityType_id`) REFERENCES `entityType` (`id`))";

		Connection connection;
		try {
			connection = this.connectionFactory.criarConexao();
			PreparedStatement ps = connection
					.prepareStatement(createDatabaseCommand);
			ps.execute();

			ps = connection.prepareStatement(createEntityTypeTable);
			ps.execute();

			// ps = connection.prepareStatement(createPropertyTypeTable);
			// ps.execute();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void dropDatabaseSchema() {
		String sql = "drop schema lom";

		Connection connection;
		try {
			connection = this.connectionFactory.criarConexao();
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.execute();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
