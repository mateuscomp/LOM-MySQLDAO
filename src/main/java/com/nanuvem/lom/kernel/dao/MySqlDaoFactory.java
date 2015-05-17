package com.nanuvem.lom.kernel.dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.nanuvem.lom.api.dao.DaoFactory;
import com.nanuvem.lom.api.dao.EntityDao;
import com.nanuvem.lom.api.dao.EntityTypeDao;
import com.nanuvem.lom.api.dao.PropertyDao;
import com.nanuvem.lom.api.dao.PropertyTypeDao;

public class MySqlDaoFactory implements DaoFactory {

	private MySqlConnector mySqlConnector;

	private MySqlEntityTypeDao entityTypeDao;
	private MySqlPropertyTypeDao propertyTypeDao;
	private MySqlEntityDao entityDao;
	private MySqlPropertyDao propertyDao;

	public MySqlDaoFactory() {
		try {
			this.mySqlConnector = new MySqlConnector();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public EntityTypeDao createEntityTypeDao() {
		if (this.entityTypeDao == null) {
			this.entityTypeDao = new MySqlEntityTypeDao(this.mySqlConnector);
		}
		return this.entityTypeDao;
	}

	public PropertyTypeDao createPropertyTypeDao() {
		if (propertyTypeDao == null) {
			this.propertyTypeDao = new MySqlPropertyTypeDao(
					this.mySqlConnector, this.createEntityTypeDao());
		}
		return this.propertyTypeDao;
	}

	public EntityDao createEntityDao() {
		if (this.entityDao == null) {
			this.entityDao = new MySqlEntityDao(this.mySqlConnector,
					this.createEntityTypeDao());
		}
		return this.entityDao;
	}

	public PropertyDao createPropertyDao() {
		if (this.propertyDao == null) {
			this.propertyDao = new MySqlPropertyDao(this.mySqlConnector,
					this.createPropertyTypeDao(), this.createEntityDao());
		}
		return this.propertyDao;
	}

	public void createDatabaseSchema() {
		String databaseName = mySqlConnector.getDatabaseName();
		this.mySqlConnector.setDatabaseName(null);

		String createDatabaseCommand = "CREATE DATABASE " + databaseName + "; ";

		String createEntityTypeTable = "CREATE TABLE " + databaseName + "."
				+ MySqlEntityTypeDao.TABLE_NAME + " ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
				+ "`version` int(11) NOT NULL DEFAULT '0', "
				+ "`namespace` varchar(45) NOT NULL, "
				+ "`name` varchar(45) DEFAULT NULL, PRIMARY KEY (`id`)); ";

		String createPropertyTypeTable = "CREATE TABLE " + databaseName + "."
				+ MySqlPropertyTypeDao.TABLE_NAME + " ("
				+ "  `id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "  `version` int(11) NOT NULL DEFAULT '0',"
				+ "  `sequence` varchar(45) DEFAULT NULL,"
				+ "  `name` varchar(45) NOT NULL,"
				+ "  `configuration` longtext,"
				+ "  `entityType_id` bigint(20) NOT NULL,"
				+ "  `type` varchar(45) NOT NULL," + "  PRIMARY KEY (`id`),"
				+ "  KEY `fk_attributeType_entityType_idx` (`entityType_id`),"
				+ "  CONSTRAINT `fk_attributeType_entityType` "
				+ "FOREIGN KEY (`entityType_id`) " + "REFERENCES "
				+ MySqlEntityTypeDao.TABLE_NAME
				+ " (`id`) ON DELETE NO ACTION ON UPDATE NO ACTION); ";

		String createEntityTable = "CREATE TABLE " + databaseName + "."
				+ MySqlEntityDao.TABLE_NAME + " ("
				+ "`id` bigint(20) NOT NULL AUTO_INCREMENT,"
				+ "  `version` int(11) NOT NULL DEFAULT '0',"
				+ "  `entityType_id` bigint(20) NOT NULL,"
				+ "  PRIMARY KEY (`id`)) ;";

		String createPropertyTable = "CREATE TABLE "
				+ databaseName
				+ "."
				+ MySqlPropertyDao.TABLE_NAME
				+ " ("
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
			connection = this.mySqlConnector.createConnection();
			PreparedStatement ps = connection
					.prepareStatement(createDatabaseCommand);
			ps.execute();
			this.mySqlConnector.setDatabaseName(databaseName);

			connection = this.mySqlConnector.createConnection();
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
		String sql = "drop schema " + mySqlConnector.getDatabaseName();

		Connection connection;
		try {
			connection = this.mySqlConnector.createConnection();
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.execute();
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
