package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.nanuvem.lom.api.Property;
import com.nanuvem.lom.api.dao.PropertyTypeDao;
import com.nanuvem.lom.api.dao.PropertyDao;
import com.nanuvem.lom.api.dao.EntityDao;

public class MySqlPropertyDao extends AbstractRelationalDAO implements
		PropertyDao {

	public static final String TABLE_NAME = "property";

	private EntityDao entityDao;
	private PropertyTypeDao propertyTypeDao;

	public MySqlPropertyDao(MySqlConnector connectionFactory,
			PropertyTypeDao propertyTypeDao, EntityDao entityDao) {
		super(connectionFactory);

		this.propertyTypeDao = propertyTypeDao;
		this.entityDao = entityDao;
	}

	public Property create(Property property) {
		String sqlInsert = "INSERT INTO "
				+ getDatabaseName()
				+ "."
				+ TABLE_NAME
				+ "(version, entity_id, propertyType_id, value) VALUES (?, ?, ?, ?);";

		try {
			Connection connection = this.createConnection();
			PreparedStatement ps = connection.prepareStatement(sqlInsert);
			ps.setInt(1, 0);
			ps.setLong(2, property.getEntity().getId());
			ps.setLong(3, property.getPropertyType().getId());
			ps.setString(4, property.getValue());
			ps.execute();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return this.findByMaxId();
	}

	private Property findByMaxId() {
		String sql = "SELECT p.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " p WHERE p.id = (SELECT max(pp.id) FROM "
				+ getDatabaseName() + "." + TABLE_NAME + " pp);";

		Property property = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			property = new Property();
			property.setId(resultSet.getLong("id"));
			property.setVersion(resultSet.getInt("version"));
			property.setValue(resultSet.getString("value"));
			property.setEntity(entityDao.findInstanceById(resultSet
					.getLong("entity_id")));
			property.setPropertyType(propertyTypeDao
					.findPropertyTypeById(resultSet.getLong("propertyType_id")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return property;
	}

	public Property update(Property property) {
		String sql = "UPDATE "
				+ getDatabaseName()
				+ "."
				+ TABLE_NAME
				+ " SET version = ?, entity_id = ?, propertyType_id = ?, value = ? where id = ?;";

		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, property.getVersion() + 1);
			ps.setLong(2, property.getEntity().getId());
			ps.setLong(3, property.getPropertyType().getId());
			ps.setString(4, property.getValue());
			ps.setLong(5, property.getId());
			ps.executeUpdate();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return findPropertyById(property.getId());
	}

	private Property findPropertyById(Long id) {
		String sql = "SELECT p.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " p WHERE p.id = ?;";

		Property property = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			property = new Property();
			property.setId(resultSet.getLong("id"));
			property.setVersion(resultSet.getInt("version"));
			property.setValue(resultSet.getString("value"));
			property.setEntity(this.entityDao.findInstanceById(resultSet
					.getLong("entity_id")));
			property.setPropertyType(this.propertyTypeDao
					.findPropertyTypeById(resultSet.getLong("propertyType_id")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return property;
	}
}