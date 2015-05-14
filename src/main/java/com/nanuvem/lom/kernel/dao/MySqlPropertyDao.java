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

	private EntityDao entityDao;
	private PropertyTypeDao propertyTypeDao;

	public MySqlPropertyDao(MySqlConnectionFactory connectionFactory,
			PropertyTypeDao propertyTypeDao, EntityDao entityDao) {
		super(connectionFactory);
		connectionFactory.setDatabaseName("lom");

		this.propertyTypeDao = propertyTypeDao;
		this.entityDao = entityDao;
	}

	public Property create(Property property) {
		String sqlInsert = "INSERT INTO "
				+ getDatabaseName()
				+ "."
				+ getNameTable()
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
		String sql = "SELECT p.* FROM " + getDatabaseName() + "."
				+ getNameTable() + " p WHERE p.id = (SELECT max(pp.id) FROM "
				+ getDatabaseName() + "." + getNameTable() + " pp);";

		Property value = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			value = new Property();
			value.setId(resultSet.getLong("id"));
			value.setVersion(resultSet.getInt("version"));
			value.setEntity(entityDao.findInstanceById(resultSet
					.getLong("entity_id")));
			value.setPropertyType(propertyTypeDao
					.findPropertyTypeById(resultSet.getLong("propertyType_id")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return value;
	}

	public Property update(Property property) {
		String sql = "UPDATE "
				+ getDatabaseName()
				+ "."
				+ getNameTable()
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
		String sql = "SELECT p.* FROM " + getDatabaseName() + "."
				+ getNameTable() + " p WHERE p.id = ?;";

		Property value = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			value = new Property();
			value.setId(resultSet.getLong("id"));
			value.setVersion(resultSet.getInt("version"));
			value.setEntity(this.entityDao.findInstanceById(resultSet
					.getLong("entity_id")));
			value.setPropertyType(this.propertyTypeDao
					.findPropertyTypeById(resultSet.getLong("propertyType_id")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return value;
	}

	@Override
	public String getNameTable() {
		return "property";
	}

}