package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.Entity;
import com.nanuvem.lom.api.dao.EntityDao;

public class MySqlEntityDao extends AbstractRelationalDAO implements EntityDao {

	public MySqlEntityDao(MySqlConnectionFactory connectionFactory) {
		super(connectionFactory);
		connectionFactory.setDatabaseName("lom");
	}

	public Entity create(Entity entity) {
		String sqlInsert = "INSERT INTO " + getDatabaseName() + "."
				+ getNameTable()
				+ " (version, name, namespace) VALUES (?, ?, ?);";

		try {
			Connection connection = this.criarConexao();
			PreparedStatement ps = connection.prepareStatement(sqlInsert);
			ps.setInt(1, 0);
			ps.setString(2, entity.getName());
			ps.setString(3, entity.getNamespace());
			ps.execute();
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return this.findEntityByMaxId();
	}

	private Entity findEntityByMaxId() {
		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + " et WHERE et.id = (SELECT max(e.id) FROM "
				+ getDatabaseName() + "." + getNameTable() + " e);";

		Entity entity = null;
		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			entity = new Entity();
			entity.setId(resultSet.getLong("id"));
			entity.setVersion(resultSet.getInt("version"));
			entity.setNamespace(resultSet.getString("namespace"));
			entity.setName(resultSet.getString("name"));
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public List<Entity> listAll() {
		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + ";";

		List<Entity> entities = new ArrayList<Entity>();
		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();
			while (resultSet.next()) {
				Entity entity = new Entity();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setNamespace(resultSet.getString("namespace"));
				entity.setName(resultSet.getString("name"));
				entities.add(entity);
			}
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entities;
	}

	public Entity findById(Long id) {
		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + " et WHERE et.id = ?;";

		Entity entity = null;
		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();
			if (resultSet.next()) {
				entity = new Entity();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setNamespace(resultSet.getString("namespace"));
				entity.setName(resultSet.getString("name"));
			}
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public List<Entity> listByFullName(String fragment) {

		String namespace = (fragment != null && !fragment.isEmpty()) ? fragment
				.substring(0, fragment.lastIndexOf(".")) : "";

		String name = (fragment != null && !fragment.isEmpty()) ? fragment
				.substring(fragment.lastIndexOf(".") + 1, fragment.length())
				: "";

		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + " et WHERE et.namespace = ? AND et.name=?;";

		List<Entity> entities = null;
		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, namespace);
			ps.setString(2, name);

			ResultSet resultSet = ps.executeQuery();
			entities = new ArrayList<Entity>();
			while (resultSet.next()) {
				Entity entity = new Entity();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setNamespace(resultSet.getString("namespace"));
				entity.setName(resultSet.getString("name"));
				entities.add(entity);
			}
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entities;
	}

	public Entity findByFullName(String fullName) {
		List<Entity> entities = this.listByFullName(fullName);

		if (entities != null && entities.size() > 0) {
			return entities.get(0);
		}
		return null;
	}

	public Entity update(Entity entity) {
		String sql = "UPDATE " + getDatabaseName() + "." + getNameTable()
				+ " SET version = ?, namespace = ?, name = ? where id = ?;";

		try {
			Connection connection = this.criarConexao();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, entity.getVersion() + 1);
			ps.setString(2, entity.getNamespace());
			ps.setString(3, entity.getName());
			ps.setLong(4, entity.getId());
			ps.executeUpdate();
			this.fecharConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return findById(entity.getId());
	}

	public void delete(Long id) {
		// TODO Auto-generated method stub
	}

	@Override
	public String getNameTable() {
		return "entityType";
	}

}
