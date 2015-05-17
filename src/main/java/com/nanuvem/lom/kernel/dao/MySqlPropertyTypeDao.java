package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.EntityType;
import com.nanuvem.lom.api.PropertyType;
import com.nanuvem.lom.api.Type;
import com.nanuvem.lom.api.dao.EntityTypeDao;
import com.nanuvem.lom.api.dao.PropertyTypeDao;

public class MySqlPropertyTypeDao extends AbstractRelationalDAO implements
		PropertyTypeDao {

	public static final String TYPE_COLUMN = "type";
	public static final String CONFIGURATION_COLUMN = "configuration";
	public static final String NAME_COLUMN = "name";
	public static final String SEQUENCE_COLUMN = "sequence";
	public static final String VERSION_COLUMN = "version";
	public static final String ID_COLUMN = "id";
	public static final String ENTITY_TYPE_COLUMN = "entityType_id";

	public static final String TABLE_NAME = "propertyType";

	private EntityTypeDao entityTypeDAO;

	public MySqlPropertyTypeDao(MySqlConnector connectionFactory,
			EntityTypeDao entityDAO) {
		super(connectionFactory);
		this.entityTypeDAO = entityDAO;
	}

	public PropertyType create(PropertyType propertyType) {
		String sqlInsert = "INSERT INTO " + getDatabaseName() + "."
				+ TABLE_NAME + " (" + VERSION_COLUMN + ", " + SEQUENCE_COLUMN
				+ ", " + NAME_COLUMN + ", " + CONFIGURATION_COLUMN + ", "
				+ ENTITY_TYPE_COLUMN + ", " + TYPE_COLUMN
				+ ") VALUES (?, ?, ?, ?, ?, ?);";

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
		return this.findPropertyTypeByMaxId();
	}

	private PropertyType findPropertyTypeByMaxId() {
		String sql = "SELECT pt.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " pt WHERE pt." + ID_COLUMN + " = " + "(SELECT MAX(pta."
				+ ID_COLUMN + ") FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " pta);";

		PropertyType propertyType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			propertyType = loadPropertyTypeWithoutEntityType(resultSet);
			propertyType.setEntityType(entityTypeDAO.findById(resultSet
					.getLong(ENTITY_TYPE_COLUMN)));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return propertyType;
	}

	public PropertyType findPropertyTypeById(Long id) {
		String sql = "SELECT pt.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " pt WHERE pt." + ID_COLUMN + " = ?;";

		PropertyType propertyType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);
			ResultSet resultSet = ps.executeQuery();
			if (resultSet.next()) {
				propertyType = loadPropertyTypeWithoutEntityType(resultSet);
				propertyType.setEntityType(entityTypeDAO.findById(resultSet
						.getLong(ENTITY_TYPE_COLUMN)));
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return propertyType;
	}

	public PropertyType findPropertyTypeByNameAndEntityTypeFullName(
			String nameAttribute, String classFullName) {

		String namespace = classFullName != null ? classFullName.substring(0,
				classFullName.lastIndexOf(".")) : "";

		String name = classFullName != null ? classFullName.substring(
				classFullName.lastIndexOf(".") + 1, classFullName.length())
				: "";

		String sql = "SELECT pt.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " pt INNER JOIN " + getDatabaseName() + "."
				+ MySqlEntityTypeDao.TABLE_NAME + " et ON pt."
				+ ENTITY_TYPE_COLUMN + " = et.id WHERE pt." + NAME_COLUMN
				+ " = ? AND et.namespace = ? AND et.name = ?";

		PropertyType propertyType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, nameAttribute);
			ps.setString(2, namespace);
			ps.setString(3, name);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				propertyType = loadPropertyTypeWithoutEntityType(resultSet);
				propertyType.setEntityType(entityTypeDAO.findById(resultSet
						.getLong(ENTITY_TYPE_COLUMN)));
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

		String sql = "SELECT pt.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " pt INNER JOIN " + getDatabaseName() + "."
				+ MySqlEntityTypeDao.TABLE_NAME + " et ON pt."
				+ ENTITY_TYPE_COLUMN
				+ " = et.id WHERE et.name = ? AND et.namespace = ? ;";

		List<PropertyType> propertiesTypes = new ArrayList<PropertyType>();
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, name);
			ps.setString(2, namespace);
			ResultSet resultSet = ps.executeQuery();

			while (resultSet.next()) {
				PropertyType propertyType = new PropertyType();
				propertyType = loadPropertyTypeWithoutEntityType(resultSet);
				propertyType.setEntityType(entityTypeDAO.findById(resultSet
						.getLong(ENTITY_TYPE_COLUMN)));
				propertiesTypes.add(propertyType);
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return propertiesTypes;

	}

	public PropertyType update(PropertyType attribute) {
		String sql = "UPDATE " + getDatabaseName() + "." + TABLE_NAME + " SET "
				+ VERSION_COLUMN + " = ?, " + SEQUENCE_COLUMN + " = ?, "
				+ NAME_COLUMN + " = ?, " + CONFIGURATION_COLUMN + " = ?, "
				+ ENTITY_TYPE_COLUMN + " = ? where " + ID_COLUMN + " = ?;";

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

	private static PropertyType loadPropertyTypeWithoutEntityType(
			ResultSet resultSet) throws SQLException {

		PropertyType propertyType;
		propertyType = new PropertyType();
		propertyType.setId(resultSet.getLong(ID_COLUMN));
		propertyType.setVersion(resultSet.getInt(VERSION_COLUMN));
		propertyType.setSequence(resultSet.getInt(SEQUENCE_COLUMN));
		propertyType.setName(resultSet.getString(NAME_COLUMN));
		propertyType
				.setConfiguration(resultSet.getString(CONFIGURATION_COLUMN));
		propertyType.setType(Type.getType(resultSet.getString(TYPE_COLUMN)));
		return propertyType;
	}

	public static List<PropertyType> loadPropertiesTypesByEntityType(
			Connection connection, EntityType entity, String databaseName) {

		String fullname = entity.getFullName();

		String namespace = fullname != null ? fullname.substring(0,
				fullname.lastIndexOf(".")) : "";

		String name = fullname != null ? fullname.substring(
				fullname.lastIndexOf(".") + 1, fullname.length()) : "";

		String sql = "SELECT pt.* FROM " + databaseName + "." + TABLE_NAME
				+ " pt INNER JOIN " + databaseName + "."
				+ MySqlEntityTypeDao.TABLE_NAME + " et ON pt."
				+ ENTITY_TYPE_COLUMN + " = et." + MySqlEntityTypeDao.ID_COLUMN
				+ " WHERE et." + MySqlEntityTypeDao.NAME_COLUMN
				+ " = ? AND et." + MySqlEntityTypeDao.NAMESPACE_COLUMN
				+ " = ? ;";

		List<PropertyType> attributes = new ArrayList<PropertyType>();
		try {
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, name);
			ps.setString(2, namespace);
			ResultSet resultSet = ps.executeQuery();

			while (resultSet.next()) {
				PropertyType propertyType = loadPropertyTypeWithoutEntityType(resultSet);
				attributes.add(propertyType);
			}
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return attributes;
	}
}