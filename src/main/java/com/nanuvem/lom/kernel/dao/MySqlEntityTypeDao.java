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
import com.nanuvem.lom.api.Type;
import com.nanuvem.lom.api.dao.EntityTypeDao;

public class MySqlEntityTypeDao extends AbstractRelationalDAO implements
		EntityTypeDao {

	public static final String TABLE_NAME = "entityType";

	public MySqlEntityTypeDao(MySqlConnector connector) {
		super(connector);
	}

	public EntityType create(EntityType entityType) {
		String sqlInsert = "INSERT INTO " + getDatabaseName() + "."
				+ TABLE_NAME + " (version, name, namespace) VALUES (?, ?, ?);";

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
				+ " et WHERE et.id = (SELECT max(e.id) FROM "
				+ getDatabaseName() + "." + TABLE_NAME + " e);";

		EntityType entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				entity = new EntityType();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setNamespace(resultSet.getString("namespace"));
				entity.setName(resultSet.getString("name"));
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
				EntityType entityType = new EntityType();
				entityType.setId(resultSet.getLong("id"));
				entityType.setVersion(resultSet.getInt("version"));
				entityType.setNamespace(resultSet.getString("namespace"));
				entityType.setName(resultSet.getString("name"));
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
				+ " et WHERE et.id = ?;";

		EntityType entityType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();
			if (resultSet.next()) {
				entityType = new EntityType();
				entityType.setId(resultSet.getLong("id"));
				entityType.setVersion(resultSet.getInt("version"));
				entityType.setNamespace(resultSet.getString("namespace"));
				entityType.setName(resultSet.getString("name"));
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
				+ " et WHERE et.namespace LIKE '%" + fragment
				+ "%' OR et.name LIKE '%" + fragment
				+ "%' OR CONCAT(et.namespace, '.', et.name) LIKE '%" + fragment
				+ "%'";

		List<EntityType> entitiesTypes = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);

			ResultSet resultSet = ps.executeQuery();
			entitiesTypes = new ArrayList<EntityType>();
			while (resultSet.next()) {
				EntityType entityType = new EntityType();
				entityType.setId(resultSet.getLong("id"));
				entityType.setVersion(resultSet.getInt("version"));
				entityType.setNamespace(resultSet.getString("namespace"));
				entityType.setName(resultSet.getString("name"));
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
				+ " et WHERE et.namespace = ? AND et.name = ? ;";

		EntityType entityType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, namespace);
			ps.setString(2, name);

			ResultSet resultSet = ps.executeQuery();
			if (resultSet.next()) {
				entityType = new EntityType();
				entityType.setId(resultSet.getLong("id"));
				entityType.setVersion(resultSet.getInt("version"));
				entityType.setNamespace(resultSet.getString("namespace"));
				entityType.setName(resultSet.getString("name"));
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

		String sql = "UPDATE " + getDatabaseName() + "." + TABLE_NAME
				+ " SET version = ?, namespace = ?, name = ? where id = ?;";

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
				+ " WHERE id = ?;";

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

	private List<PropertyType> getPropertiesTypes(EntityType entity) {
		String fullname = entity.getFullName();

		String namespace = fullname != null ? fullname.substring(0,
				fullname.lastIndexOf(".")) : "";
		String name = fullname != null ? fullname.substring(
				fullname.lastIndexOf(".") + 1, fullname.length()) : "";

		String sql = "SELECT pt.* FROM "
				+ getDatabaseName()
				+ "."
				+ MySqlPropertyTypeDao.TABLE_NAME
				+ " pt INNER JOIN "
				+ getDatabaseName()
				+ "."
				+ TABLE_NAME
				+ " et ON pt.entityType_id = et.id WHERE et.name = ? AND et.namespace = ? ;";

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
}
