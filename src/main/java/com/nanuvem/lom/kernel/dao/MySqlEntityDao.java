package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.Entity;
import com.nanuvem.lom.api.dao.EntityTypeDao;
import com.nanuvem.lom.api.dao.EntityDao;

public class MySqlEntityDao extends AbstractRelationalDAO implements
		EntityDao {

	private EntityTypeDao entityDAO;

	public MySqlEntityDao(MySqlConnectionFactory connectionFactory,
			EntityTypeDao entityDAO) {
		super(connectionFactory);
		connectionFactory.setDatabaseName("lom");
		this.entityDAO = entityDAO;
	}

	public Entity create(Entity entity) {
		String sqlInsert = "INSERT INTO " + getDatabaseName() + "."
				+ getNameTable() + "(version, entityType_id) VALUES (?, ?);";

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
		String sql = "SELECT e.* FROM " + getDatabaseName() + "."
				+ getNameTable() + " e WHERE e.id = (SELECT max(ee.id) FROM "
				+ getDatabaseName() + "." + getNameTable() + " ee);";

		Entity entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			entity = new Entity();
			entity.setId(resultSet.getLong("id"));
			entity.setVersion(resultSet.getInt("version"));
			entity.setEntityType(entityDAO.findById(resultSet
					.getLong("entityType_id")));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public Entity findInstanceById(Long id) {
		String sql = "SELECT e.* FROM " + getDatabaseName() + "."
				+ getNameTable() + " e WHERE e.id = ?;";

		Entity entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			entity = new Entity();
			entity.setId(resultSet.getLong("id"));
			entity.setVersion(resultSet.getInt("version"));
			entity.setEntityType(entityDAO.findById(resultSet
					.getLong("entityType_id")));

			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public List<Entity> findInstancesByEntityId(Long entityId) {
		MySqlEntityTypeDao mySqlEntityDao = (MySqlEntityTypeDao) this.entityDAO;

		String sql = "SELECT e.* FROM " + getDatabaseName() + "."
				+ getNameTable() + " e  INNER JOIN " + getDatabaseName() + "."
				+ mySqlEntityDao.getNameTable()
				+ " et ON e.entityType_id = e.id WHERE et.id = ?;";

		List<Entity> instancies;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, entityId);

			ResultSet resultSet = ps.executeQuery();
			instancies = new ArrayList<Entity>();
			if (resultSet.next()) {
				Entity entity = new Entity();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setEntityType(entityDAO.findById(resultSet
						.getLong("entityType_id")));
				instancies.add(entity);
			}

			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return instancies;
	}

	public Entity update(Entity entity) {
		String sql = "UPDATE " + getDatabaseName() + "." + getNameTable()
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

	@Override
	public String getNameTable() {
		return "entity";
	}

}