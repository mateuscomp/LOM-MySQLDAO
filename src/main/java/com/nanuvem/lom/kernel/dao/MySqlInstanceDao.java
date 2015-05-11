package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.Instance;
import com.nanuvem.lom.api.dao.EntityDao;
import com.nanuvem.lom.api.dao.InstanceDao;

public class MySqlInstanceDao extends AbstractRelationalDAO implements
		InstanceDao {

	private EntityDao entityDAO;

	public MySqlInstanceDao(MySqlConnectionFactory connectionFactory,
			EntityDao entityDAO) {
		super(connectionFactory);
//		connectionFactory.setDatabaseName("lom");
		this.entityDAO = entityDAO;
	}

	public Instance create(Instance instance) {
		String sqlInsert = "INSERT INTO " + getNameTable()
				+ "(version, entityType_id) VALUES (?, ?);";

		try {
			Connection connection = this.criarConexao();
			PreparedStatement ps = connection.prepareStatement(sqlInsert);
			ps.setInt(1, 0);
			ps.setLong(2, instance.getEntity().getId());
			ps.execute();
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return this.findInstanceByMaxId();
	}

	private Instance findInstanceByMaxId() {
		String sql = "SELECT * FROM " + getNameTable()
				+ " e WHERE e.id = (SELECT max(ee.id) FROM " + getNameTable()
				+ " ee);";

		Instance instance = null;
		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			instance = new Instance();
			instance.setId(resultSet.getLong("id"));
			instance.setVersion(resultSet.getInt("version"));
			instance.setEntity(entityDAO.findById(resultSet
					.getLong("entityType_id")));
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return instance;
	}

	public Instance findInstanceById(Long id) {
		String sql = "SELECT * FROM " + getNameTable() + " e WHERE e.id = ?;";

		Instance instance = null;
		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			instance = new Instance();
			instance.setId(resultSet.getLong("id"));
			instance.setVersion(resultSet.getInt("version"));
			instance.setEntity(entityDAO.findById(resultSet
					.getLong("entityType_id")));

			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return instance;
	}

	public List<Instance> findInstancesByEntityId(Long entityId) {
		MySqlEntityDao mySqlEntityDao = (MySqlEntityDao) this.entityDAO;

		String sql = "SELECT * FROM " + getNameTable() + " e  INNER JOIN "
				+ mySqlEntityDao.getNameTable()
				+ "et ON e.entityType_id = e.id WHERE et.id = ?;";

		List<Instance> instancies;
		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, entityId);

			ResultSet resultSet = ps.executeQuery();
			instancies = new ArrayList<Instance>();
			if (resultSet.next()) {
				Instance instance = new Instance();
				instance.setId(resultSet.getLong("id"));
				instance.setVersion(resultSet.getInt("version"));
				instance.setEntity(entityDAO.findById(resultSet
						.getLong("entityType_id")));
				instancies.add(instance);
			}

			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return instancies;
	}

	public Instance update(Instance instance) {
		String sql = "UPDATE " + getNameTable()
				+ " SET version = ?, entityType_id = ? where id = ?;";

		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, instance.getVersion());
			ps.setLong(2, instance.getEntity().getId());
			ps.setLong(3, instance.getId());
			ps.executeUpdate();
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return findInstanceById(instance.getId());
	}

	public void delete(Long id) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getNameTable() {
		return "entity";
	}

}