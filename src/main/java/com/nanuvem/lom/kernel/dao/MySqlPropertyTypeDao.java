package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.PropertyType;
import com.nanuvem.lom.api.Type;
import com.nanuvem.lom.api.dao.PropertyTypeDao;
import com.nanuvem.lom.api.dao.EntityTypeDao;

public class MySqlPropertyTypeDao extends AbstractRelationalDAO implements
		PropertyTypeDao {

	private EntityTypeDao entityDAO;

	public MySqlPropertyTypeDao(MySqlConnectionFactory connectionFactory,
			EntityTypeDao entityDAO) {
		super(connectionFactory);
		connectionFactory.setDatabaseName("lom");
		this.entityDAO = entityDAO;
	}

	public PropertyType create(PropertyType attribute) {
		String sqlInsert = "INSERT INTO "
				+ getDatabaseName()
				+ "."
				+ getNameTable()
				+ " (version, sequence, name, configuration, entityType_id, type) VALUES (?, ?, ?, ?, ?, ?);";

		try {
			Connection connection = this.createConnection();
			PreparedStatement ps = connection.prepareStatement(sqlInsert);
			ps.setInt(1, 0);
			ps.setInt(2,
					attribute.getSequence() != null ? attribute.getSequence()
							: 0);
			ps.setString(3, attribute.getName());
			ps.setString(
					4,
					attribute.getConfiguration() != null ? attribute
							.getConfiguration() : "");
			ps.setLong(5, attribute.getEntityType().getId());
			ps.setString(6, attribute.getType().toString());
			ps.execute();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return this.findAttributeByMaxId();
	}

	private PropertyType findAttributeByMaxId() {
		String sql = "SELECT at.* FROM " + getDatabaseName() + "."
				+ getNameTable() + " at WHERE at.id = "
				+ "(SELECT max(a.id) FROM " + getDatabaseName() + "."
				+ getNameTable() + " a);";

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
			attribute
					.setType(Type.getType(resultSet.getString("type")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return attribute;
	}

	public PropertyType findPropertyTypeById(Long id) {
		String sql = "SELECT at.* FROM " + getDatabaseName() + "."
				+ getNameTable() + " at WHERE at.id = ?;";

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
			attribute.setEntityType(entityDAO.findById(resultSet.getLong("id")));
			attribute
					.setType(Type.getType(resultSet.getString("type")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return attribute;
	}

	public PropertyType findPropertyTypeByNameAndEntityTypeFullName(String nameAttribute,
			String classFullName) {

		MySqlEntityTypeDao mysqlEntityDao = (MySqlEntityTypeDao) this.entityDAO;

		String namespace = classFullName != null ? classFullName.substring(0,
				classFullName.lastIndexOf(".")) : "";
		String name = classFullName != null ? classFullName.substring(
				classFullName.lastIndexOf(".") + 1, classFullName.length())
				: "";

		String sql = "SELECT at.* FROM "
				+ getDatabaseName()
				+ "."
				+ getNameTable()
				+ " at INNER JOIN "
				+ getDatabaseName()
				+ "."
				+ mysqlEntityDao.getNameTable()
				+ " et ON at.entityType_id = et.id WHERE at.name = ? AND et.namespace = ? AND et.name = ?";

		PropertyType attribute = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, nameAttribute);
			ps.setString(2, namespace);
			ps.setString(3, name);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				attribute = new PropertyType();
				attribute.setId(resultSet.getLong("id"));
				attribute.setVersion(resultSet.getInt("version"));
				attribute.setSequence(resultSet.getInt("sequence"));
				attribute.setName(resultSet.getString("name"));
				attribute
						.setConfiguration(resultSet.getString("configuration"));
				attribute
						.setEntityType(entityDAO.findById(resultSet.getLong("id")));
				attribute.setType(Type.getType(resultSet
						.getString("type")));
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return attribute;
	}

	public List<PropertyType> findPropertiesTypesByFullNameEntityType(String fullnameEntity) {
		MySqlEntityTypeDao mysqlEntityDao = (MySqlEntityTypeDao) this.entityDAO;

		String namespace = fullnameEntity != null ? fullnameEntity.substring(0,
				fullnameEntity.lastIndexOf(".")) : "";
		String name = fullnameEntity != null ? fullnameEntity.substring(
				fullnameEntity.lastIndexOf(".") + 1, fullnameEntity.length())
				: "";

		String sql = "SELECT at.* FROM "
				+ getDatabaseName()
				+ "."
				+ getNameTable()
				+ " at INNER JOIN "
				+ getDatabaseName()
				+ "."
				+ mysqlEntityDao.getNameTable()
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
				attribute.setType(Type.getType(resultSet
						.getString("type")));
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
				+ getNameTable()
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

	@Override
	public String getNameTable() {
		return "propertyType";
	}
}