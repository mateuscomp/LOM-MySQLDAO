package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.PropertyType;
import com.nanuvem.lom.api.Type;
import com.nanuvem.lom.api.dao.EntityTypeDao;
import com.nanuvem.lom.api.dao.PropertyTypeDao;

public class MySqlPropertyTypeDao extends AbstractRelationalDAO implements
		PropertyTypeDao {

	public static final String TABLE_NAME = "propertyType";

	private EntityTypeDao entityDAO;

	public MySqlPropertyTypeDao(MySqlConnector connectionFactory,
			EntityTypeDao entityDAO) {
		super(connectionFactory);
		this.entityDAO = entityDAO;
	}

	public PropertyType create(PropertyType propertyType) {
		String sqlInsert = "INSERT INTO "
				+ getDatabaseName()
				+ "."
				+ TABLE_NAME
				+ " (version, sequence, name, configuration, entityType_id, type) VALUES (?, ?, ?, ?, ?, ?);";

		try {
			Connection connection = this.createConnection();
			PreparedStatement ps = connection.prepareStatement(sqlInsert);
			ps.setInt(1, 0);
			ps.setInt(
					2,
					propertyType.getSequence() != null ? propertyType
							.getSequence() : 0);
			ps.setString(3, propertyType.getName());
			ps.setString(
					4,
					propertyType.getConfiguration() != null ? propertyType
							.getConfiguration() : "");
			ps.setLong(5, propertyType.getEntityType().getId());
			ps.setString(6, propertyType.getType().toString());
			ps.execute();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return this.findAttributeByMaxId();
	}

	private PropertyType findAttributeByMaxId() {
		String sql = "SELECT at.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " at WHERE at.id = " + "(SELECT max(a.id) FROM "
				+ getDatabaseName() + "." + TABLE_NAME + " a);";

		PropertyType attribute = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			attribute = new PropertyType();
			attribute.setId(resultSet.getLong("id"));
			attribute.setVersion(resultSet.getInt("version"));
			attribute.setSequence(resultSet.getInt("sequence"));
			attribute.setName(resultSet.getString("name"));
			attribute.setConfiguration(resultSet.getString("configuration"));
			attribute.setEntityType(entityDAO.findById(resultSet
					.getLong("entityType_id")));
			attribute.setType(Type.getType(resultSet.getString("type")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return attribute;
	}

	public PropertyType findPropertyTypeById(Long id) {
		String sql = "SELECT at.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " at WHERE at.id = ?;";

		PropertyType attribute = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			attribute = new PropertyType();
			attribute.setId(resultSet.getLong("id"));
			attribute.setVersion(resultSet.getInt("version"));
			attribute.setSequence(resultSet.getInt("sequence"));
			attribute.setName(resultSet.getString("name"));
			attribute.setConfiguration(resultSet.getString("configuration"));
			attribute.setEntityType(entityDAO.findById(resultSet
					.getLong("entityType_id")));
			attribute.setType(Type.getType(resultSet.getString("type")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return attribute;
	}

	public PropertyType findPropertyTypeByNameAndEntityTypeFullName(
			String nameAttribute, String classFullName) {

		String namespace = classFullName != null ? classFullName.substring(0,
				classFullName.lastIndexOf(".")) : "";

		String name = classFullName != null ? classFullName.substring(
				classFullName.lastIndexOf(".") + 1, classFullName.length())
				: "";

		String sql = "SELECT pt.* FROM "
				+ getDatabaseName()
				+ "."
				+ TABLE_NAME
				+ " pt INNER JOIN "
				+ getDatabaseName()
				+ "."
				+ MySqlEntityTypeDao.TABLE_NAME
				+ " et ON pt.entityType_id = et.id WHERE pt.name = ? AND et.namespace = ? AND et.name = ?";

		PropertyType propertyType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, nameAttribute);
			ps.setString(2, namespace);
			ps.setString(3, name);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				propertyType = new PropertyType();
				propertyType.setId(resultSet.getLong("id"));
				propertyType.setVersion(resultSet.getInt("version"));
				propertyType.setSequence(resultSet.getInt("sequence"));
				propertyType.setName(resultSet.getString("name"));
				propertyType.setConfiguration(resultSet
						.getString("configuration"));
				propertyType.setEntityType(entityDAO.findById(resultSet
						.getLong("entityType_id")));
				propertyType.setType(Type.getType(resultSet.getString("type")));
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return propertyType;
	}

	public List<PropertyType> findPropertiesTypesByFullNameEntityType(
			String fullnameEntity) {

		String namespace = fullnameEntity != null ? fullnameEntity.substring(0,
				fullnameEntity.lastIndexOf(".")) : "";
		String name = fullnameEntity != null ? fullnameEntity.substring(
				fullnameEntity.lastIndexOf(".") + 1, fullnameEntity.length())
				: "";

		String sql = "SELECT at.* FROM "
				+ getDatabaseName()
				+ "."
				+ TABLE_NAME
				+ " at INNER JOIN "
				+ getDatabaseName()
				+ "."
				+ MySqlEntityTypeDao.TABLE_NAME
				+ " et ON at.entityType_id = et.id WHERE et.name = ? AND et.namespace = ? ;";

		List<PropertyType> attributes = new ArrayList<PropertyType>();
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, name);
			ps.setString(2, namespace);
			ResultSet resultSet = ps.executeQuery();

			while (resultSet.next()) {
				PropertyType attribute = new PropertyType();
				attribute = new PropertyType();
				attribute.setId(resultSet.getLong("id"));
				attribute.setVersion(resultSet.getInt("version"));
				attribute.setSequence(resultSet.getInt("sequence"));
				attribute.setName(resultSet.getString("name"));
				attribute
						.setConfiguration(resultSet.getString("configuration"));
				attribute.setEntityType(entityDAO.findById(resultSet
						.getLong("entityType_id")));
				attribute.setType(Type.getType(resultSet.getString("type")));
				attributes.add(attribute);
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return attributes;

	}

	public PropertyType update(PropertyType attribute) {
		String sql = "UPDATE "
				+ getDatabaseName()
				+ "."
				+ TABLE_NAME
				+ " SET version = ?, sequence = ?, name = ?, configuration = ?, entityType_id = ? where id = ?;";

		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, attribute.getVersion() + 1);
			ps.setInt(
					2,
					attribute.getSequence() == null ? 0 : attribute
							.getSequence());
			ps.setString(3, attribute.getName());
			ps.setString(4, attribute.getConfiguration());
			ps.setLong(5, attribute.getEntityType().getId());
			ps.setLong(6, attribute.getId());
			ps.executeUpdate();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return findPropertyTypeById(attribute.getId());
	}
}