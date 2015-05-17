package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.Entity;
import com.nanuvem.lom.api.EntityType;
import com.nanuvem.lom.api.Property;
import com.nanuvem.lom.api.PropertyType;
import com.nanuvem.lom.api.Type;
import com.nanuvem.lom.api.dao.EntityDao;
import com.nanuvem.lom.api.dao.EntityTypeDao;

public class MySqlEntityDao extends AbstractRelationalDAO implements EntityDao {

	public static final String TABLE_NAME = "entity";

	private EntityTypeDao entityTypeDAO;

	public MySqlEntityDao(MySqlConnector connectionFactory,
			EntityTypeDao entityTypeDAO) {
		super(connectionFactory);

		this.entityTypeDAO = entityTypeDAO;
	}

	public Entity create(Entity entity) {
		String sqlInsert = "INSERT INTO " + getDatabaseName() + "."
				+ TABLE_NAME + "(version, entityType_id) VALUES (?, ?);";

		try {
			Connection connection = this.createConnection();
			PreparedStatement ps = connection.prepareStatement(sqlInsert);
			ps.setInt(1, 0);
			ps.setLong(2, entity.getEntityType().getId());
			ps.execute();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return this.findInstanceByMaxId();
	}

	private Entity findInstanceByMaxId() {
		String sql = "SELECT e.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " e WHERE e.id = (SELECT max(ee.id) FROM "
				+ getDatabaseName() + "." + TABLE_NAME + " ee);";

		Entity entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			entity = new Entity();
			entity.setId(resultSet.getLong("id"));
			entity.setVersion(resultSet.getInt("version"));

			Long idEntityType = resultSet.getLong("entityType_id");
			EntityType et = this.entityTypeDAO.findById(idEntityType);
			entity.setEntityType(et);
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public Entity findInstanceById(Long id) {
		String sql = "SELECT e.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " e WHERE e.id = ?;";

		Entity entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				entity = new Entity();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setEntityType(entityTypeDAO.findById(resultSet
						.getLong("entityType_id")));

			}
			this.closeConexao();
			if (entity != null) {
				entity.setProperties(getProperties(entity));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public List<Entity> findInstancesByEntityId(Long entityId) {
		String sql = "SELECT e.* FROM " + getDatabaseName() + "." + TABLE_NAME
				+ " e  INNER JOIN " + getDatabaseName() + "."
				+ MySqlEntityTypeDao.TABLE_NAME
				+ " et ON e.entityType_id = et.id WHERE et.id = ?;";

		List<Entity> entities;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, entityId);

			ResultSet resultSet = ps.executeQuery();
			entities = new ArrayList<Entity>();
			while (resultSet.next()) {
				Entity entity = new Entity();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setEntityType(entityTypeDAO.findById(resultSet
						.getLong("entityType_id")));
				entities.add(entity);
			}
			this.closeConexao();

			for (Entity e : entities) {
				e.setProperties(this.getProperties(e));
			}

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entities;
	}

	public Entity update(Entity entity) {
		String sql = "UPDATE " + getDatabaseName() + "." + TABLE_NAME
				+ " SET version = ?, entityType_id = ? where id = ?;";

		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, entity.getVersion() + 1);
			ps.setLong(2, entity.getEntityType().getId());
			ps.setLong(3, entity.getId());
			ps.executeUpdate();
			this.closeConexao();

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return findInstanceById(entity.getId());
	}

	public void delete(Long id) {
		// TODO Auto-generated method stub

	}

	private List<Property> getProperties(Entity entity) {
		String sql = "SELECT p.* FROM " + getDatabaseName() + "."
				+ MySqlPropertyDao.TABLE_NAME + " p WHERE p.entity_id = ?;";

		List<Property> properties = new ArrayList<Property>();
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, entity.getId());

			ResultSet resultSet = ps.executeQuery();
			while (resultSet.next()) {
				Property property = new Property();
				property = new Property();
				property.setId(resultSet.getLong("id"));
				property.setVersion(resultSet.getInt("version"));
				property.setValue(resultSet.getString("value"));
				property.setEntity(entity);
				properties.add(property);
			}
			this.closeConexao();

			for (Property p : properties) {
				p.setPropertyType(getPropertyType(p));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return properties;
	}

	private PropertyType getPropertyType(Property property) {
		String sql = "SELECT pt.* FROM " + getDatabaseName() + "."
				+ MySqlPropertyTypeDao.TABLE_NAME + " pt WHERE pt.id = ?;";

		PropertyType propertyType = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, property.getId());
			ResultSet resultSet = ps.executeQuery();

			if (resultSet.next()) {
				propertyType = new PropertyType();
				propertyType.setId(resultSet.getLong("id"));
				propertyType.setVersion(resultSet.getInt("version"));
				propertyType.setSequence(resultSet.getInt("sequence"));
				propertyType.setName(resultSet.getString("name"));
				propertyType.setConfiguration(resultSet
						.getString("configuration"));
				propertyType.setType(Type.getType(resultSet.getString("type")));

				EntityTypeDao entityTypeDao = new MySqlEntityTypeDao(
						this.connector);
				propertyType.setEntityType(entityTypeDao.findById(resultSet
						.getLong("entityType_id")));
			}
			this.closeConexao();

		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return propertyType;
	}
}