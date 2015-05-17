package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.EntityType;
import com.nanuvem.lom.api.MetadataException;
import com.nanuvem.lom.api.PropertyType;
import com.nanuvem.lom.api.dao.EntityTypeDao;

public class MySqlEntityTypeDao extends AbstractRelationalDAO implements
		EntityTypeDao {

	public static final String ID_COLUMN = "id";
	public static final String VERSION_COLUMN = "version";
	public static final String NAMESPACE_COLUMN = "namespace";
	public static final String NAME_COLUMN = "name";

	public static final String TABLE_NAME = "entityType";

	public MySqlEntityTypeDao(MySqlConnector connector) {
		super(connector);
	}

	public EntityType create(EntityType entityType) {
		String sqlInsert = "INSERT INTO " + getDatabaseName() + "."
				+ TABLE_NAME + " (" + VERSION_COLUMN + ", " + NAME_COLUMN
				+ ", " + NAMESPACE_COLUMN + ") VALUES (?, ?, ?);";

		try {
			Connection connection = this.createConnection();
			PreparedStatement ps = connection.prepareStatement(sqlInsert);
			ps.setInt(1, 0);
			ps.setString(2, entityType.getName());
			ps.setString(3, entityType.getNamespace());
			ps.execute();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return this.findByMaxId();
	}

	private EntityType findByMaxId() {
		String sql = "SELECT * FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " et WHERE et." + ID_COLUMN + " = (SELECT max(e." + ID_COLUMN
				+ ") FROM " + getDatabaseName() + "." + TABLE_NAME + " e);";

		EntityType entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				entity = loadEntityTypeWithoutPropertiesTypes(resultSet);
			}
			this.closeConexao();

			if (entity != null) {
				entity.setAttributes(getPropertiesTypes(entity));
			}

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public List<EntityType> listAll() {
		String sql = "SELECT * FROM " + getDatabaseName() + "." + TABLE_NAME
				+ ";";

		List<EntityType> entitiesTypes = new ArrayList<EntityType>();
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();
			while (resultSet.next()) {
				EntityType entityType = loadEntityTypeWithoutPropertiesTypes(resultSet);
				entitiesTypes.add(entityType);
			}
			this.closeConexao();
			for (EntityType et : entitiesTypes) {
				et.setAttributes(this.getPropertiesTypes(et));
			}

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entitiesTypes;
	}

	public EntityType findById(Long id) {
		String sql = "SELECT * FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " et WHERE et." + ID_COLUMN + " = ?;";

		EntityType entityType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();
			if (resultSet.next()) {
				entityType = loadEntityTypeWithoutPropertiesTypes(resultSet);
			}
			this.closeConexao();
			if (entityType != null) {
				entityType.setAttributes(this.getPropertiesTypes(entityType));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entityType;
	}

	public List<EntityType> listByFullName(String fragment) {
		String sql = "SELECT * FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " et WHERE et." + NAMESPACE_COLUMN + " LIKE '%" + fragment
				+ "%' OR et." + NAME_COLUMN + " LIKE '%" + fragment
				+ "%' OR CONCAT(et." + NAMESPACE_COLUMN + ", '.', et."
				+ NAME_COLUMN + ") LIKE '%" + fragment + "%'";

		List<EntityType> entitiesTypes = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);

			ResultSet resultSet = ps.executeQuery();
			entitiesTypes = new ArrayList<EntityType>();
			while (resultSet.next()) {
				EntityType entityType = loadEntityTypeWithoutPropertiesTypes(resultSet);
				entitiesTypes.add(entityType);
			}
			this.closeConexao();
			for (EntityType et : entitiesTypes) {
				et.setAttributes(this.getPropertiesTypes(et));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entitiesTypes;
	}

	public EntityType findByFullName(String fullName) {
		String namespace = null;
		try {
			namespace = (fullName != null && !fullName.isEmpty()) ? fullName
					.substring(0, fullName.lastIndexOf(".")) : "";
		} catch (StringIndexOutOfBoundsException e) {
			namespace = null;
		}

		String name;
		try {
			name = (fullName != null && !fullName.isEmpty()) ? fullName
					.substring(fullName.lastIndexOf(".") + 1, fullName.length())
					: "";

		} catch (StringIndexOutOfBoundsException e) {
			name = null;
		}

		String sql = "SELECT * FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " et WHERE et." + NAMESPACE_COLUMN + " = ? AND et."
				+ NAME_COLUMN + " = ? ;";

		EntityType entityType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, namespace);
			ps.setString(2, name);

			ResultSet resultSet = ps.executeQuery();
			if (resultSet.next()) {
				entityType = loadEntityTypeWithoutPropertiesTypes(resultSet);
			}
			this.closeConexao();
			if (entityType != null) {
				entityType.setAttributes(this.getPropertiesTypes(entityType));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return entityType;
	}

	public EntityType update(EntityType entityType) {

		EntityType entityTypePersisted = this.findById(entityType.getId());

		if (entityTypePersisted == null) {
			throw new MetadataException("Invalid id for Entity "
					+ entityType.getNamespace() + "." + entityType.getName());
		}

		else if (entityTypePersisted.getVersion() > entityType.getVersion()) {
			throw new MetadataException(
					"Updating a deprecated version of Entity "
							+ entityTypePersisted.getNamespace()
							+ "."
							+ entityTypePersisted.getName()
							+ ". Get the Entity again to obtain the newest version and proceed updating.");
		}

		entityType.setVersion(entityTypePersisted.getVersion() + 1);

		String sql = "UPDATE " + getDatabaseName() + "." + TABLE_NAME + " SET "
				+ VERSION_COLUMN + " = ?, " + NAMESPACE_COLUMN + " = ?, "
				+ NAME_COLUMN + " = ? where " + ID_COLUMN + " = ?;";

		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, entityType.getVersion());
			ps.setString(2, entityType.getNamespace());
			ps.setString(3, entityType.getName());
			ps.setLong(4, entityType.getId());
			ps.executeUpdate();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return findById(entityType.getId());
	}

	public void delete(Long id) {
		String sql = "DELETE FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " WHERE " + ID_COLUMN + " = ?;";

		try {
			Connection connection = this.createConnection();
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);
			ps.execute();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private List<PropertyType> getPropertiesTypes(EntityType entityType) {
		try {
			return MySqlPropertyTypeDao.loadPropertiesTypesByEntityType(
					this.createConnection(), entityType, getDatabaseName());
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	private EntityType loadEntityTypeWithoutPropertiesTypes(ResultSet resultSet)
			throws SQLException {
		EntityType entityType;
		entityType = new EntityType();
		entityType.setId(resultSet.getLong(ID_COLUMN));
		entityType.setVersion(resultSet.getInt(VERSION_COLUMN));
		entityType.setNamespace(resultSet.getString(NAMESPACE_COLUMN));
		entityType.setName(resultSet.getString(NAME_COLUMN));
		return entityType;
	}
}