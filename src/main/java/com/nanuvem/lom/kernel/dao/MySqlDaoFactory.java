package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.nanuvem.lom.api.dao.PropertyTypeDao;
import com.nanuvem.lom.api.dao.PropertyDao;
import com.nanuvem.lom.api.dao.DaoFactory;
import com.nanuvem.lom.api.dao.EntityTypeDao;
import com.nanuvem.lom.api.dao.EntityDao;

public class MySqlDaoFactory implements DaoFactory {

	private MySqlConnectionFactory connectionFactory;

	private MySqlEntityTypeDao entityDao;
	private MySqlPropertyTypeDao attributeDao;
	private MySqlEntityDao instanceDao;
	private MySqlPropertyDao attributeValueDao;

	public MySqlDaoFactory() {
		this.connectionFactory = new MySqlConnectionFactory(
				"connectipharma2-rds-saopaulo.cpmi3wndyz9u.sa-east-1.rds.amazonaws.com",
				"3396", "masteruserrdscp2", "GBhg65Ip5297Cv4");

		// this.connectionFactory = new MySqlConnectionFactory("localhost",
		// "3306", "root", "root");
	}

	public EntityTypeDao createEntityTypeDao() {
		if (this.entityDao == null) {
			this.entityDao = new MySqlEntityTypeDao(this.connectionFactory);
		}
		return this.entityDao;
	}

	public PropertyTypeDao createPropertyTypeDao() {
		if (attributeDao == null) {
			this.attributeDao = new MySqlPropertyTypeDao(
					this.connectionFactory, this.createEntityTypeDao());
		}
		return this.attributeDao;
	}

	public EntityDao createEntityDao() {
		if (this.instanceDao == null) {
			this.instanceDao = new MySqlEntityDao(this.connectionFactory,
					this.createEntityTypeDao());
		}
		return this.instanceDao;
	}

	public PropertyDao createPropertyDao() {
		if (this.attributeValueDao == null) {
			this.attributeValueDao = new MySqlPropertyDao(
					this.connectionFactory, this.createPropertyTypeDao(),
					this.createEntityDao());
		}
		return this.attributeValueDao;
	}

	public void createDatabaseSchema() {
		String createDatabaseCommand = "CREATE DATABASE `lom`; ";

		String createEntityTypeTable = "CREATE TABLE lom.entityType ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
				+ "`version` int(11) NOT NULL DEFAULT '0', "
				+ "`namespace` varchar(45) NOT NULL, "
				+ "`name` varchar(45) DEFAULT NULL, " + "PRIMARY KEY (`id`)); ";

		String createPropertyTypeTable = "CREATE TABLE lom.propertyType ("
				+ "  `id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "  `version` int(11) NOT NULL DEFAULT '0',"
				+ "  `sequence` varchar(45) DEFAULT NULL,"
				+ "  `name` varchar(45) NOT NULL,"
				+ "  `configuration` longtext,"
				+ "  `entityType_id` bigint(20) NOT NULL,"
				+ "  `type` varchar(45) NOT NULL,"
				+ "  PRIMARY KEY (`id`),"
				+ "  KEY `fk_attributeType_entityType_idx` (`entityType_id`),"
				+ "  CONSTRAINT `fk_attributeType_entityType` "
				+ "FOREIGN KEY (`entityType_id`) "
				+ "REFERENCES `entityType` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION); ";

		String createEntityTable = "CREATE TABLE lom.entity ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "  `version` int(11) NOT NULL DEFAULT '0',"
				+ "  `entityType_id` bigint(20) NOT NULL,"
				+ "  PRIMARY KEY (`id`)) ;";

		String createPropertyTable = "CREATE TABLE lom.property ("
				+ "  `id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "  `version` int(11) NOT NULL DEFAULT '0',"
				+ "  `entity_id` bigint(20) NOT NULL,"
				+ "  `propertyType_id` bigint(20) NOT NULL,"
				+ "  `value` varchar(255) DEFAULT NULL,"
				+ "  PRIMARY KEY (`id`),"
				+ "  KEY `fk_property_entity_idx` (`entity_id`),"
				+ "  KEY `fk_property_propertyType_idx` (`propertyType_id`),"
				+ "  CONSTRAINT `fk_property_entity` FOREIGN KEY (`entity_id`)"
				+ " REFERENCES `entity` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION,"
				+ "  CONSTRAINT `fk_property_propertyType` "
				+ "FOREIGN KEY (`propertyType_id`) "
				+ "REFERENCES `propertyType` (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION); ";

		Connection connection;
		try {
			connection = this.connectionFactory.createConnection();
			PreparedStatement ps = connection
					.prepareStatement(createDatabaseCommand);
			ps.execute();
			connection.close();

			this.connectionFactory.setDatabaseName("lom");
			connection = this.connectionFactory.createConnection();
			ps = connection.prepareStatement(createEntityTypeTable);
			ps.execute();

			ps = connection.prepareStatement(createPropertyTypeTable);
			ps.execute();

			ps = connection.prepareStatement(createEntityTable);
			ps.execute();

			ps = connection.prepareStatement(createPropertyTable);
			ps.execute();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void dropDatabaseSchema() {
		String sql = "drop schema lom";

		Connection connection;
		try {
			connection = this.connectionFactory.createConnection();
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.execute();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
