package com.nanuvem.lom.kernel.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.nanuvem.lom.api.EntityType;
import com.nanuvem.lom.api.dao.EntityTypeDao;

public class MySqlEntityTypeDao extends AbstractRelationalDAO implements EntityTypeDao {

	public MySqlEntityTypeDao(MySqlConnector connectionFactory) {
		super(connectionFactory);
		
		connectionFactory.setDatabaseName("lom");
	}

	public EntityType create(EntityType entityType) {
		String sqlInsert = "INSERT INTO " + getDatabaseName() + "."
				+ getNameTable()
				+ " (version, name, namespace) VALUES (?, ?, ?);";

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
		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + " et WHERE et.id = (SELECT max(e.id) FROM "
				+ getDatabaseName() + "." + getNameTable() + " e);";

		EntityType entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();

			resultSet.next();
			entity = new EntityType();
			entity.setId(resultSet.getLong("id"));
			entity.setVersion(resultSet.getInt("version"));
			entity.setNamespace(resultSet.getString("namespace"));
			entity.setName(resultSet.getString("name"));
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public List<EntityType> listAll() {
		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + ";";

		List<EntityType> entities = new ArrayList<EntityType>();
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet resultSet = ps.executeQuery();
			while (resultSet.next()) {
				EntityType entity = new EntityType();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setNamespace(resultSet.getString("namespace"));
				entity.setName(resultSet.getString("name"));
				entities.add(entity);
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entities;
	}

	public EntityType findById(Long id) {
		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + " et WHERE et.id = ?;";

		EntityType entity = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setLong(1, id);

			ResultSet resultSet = ps.executeQuery();
			if (resultSet.next()) {
				entity = new EntityType();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setNamespace(resultSet.getString("namespace"));
				entity.setName(resultSet.getString("name"));
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entity;
	}

	public List<EntityType> listByFullName(String fragment) {
		String sql = "SELECT * FROM " + getDatabaseName() + "."
				+ getNameTable() + " et WHERE ";

		String namespace = null;
		try {
			namespace = (fragment != null && !fragment.isEmpty()) ? fragment
					.substring(0, fragment.lastIndexOf(".")) : "";

			sql += "et.namespace LIKE '%" + namespace + "%'";
		} catch (StringIndexOutOfBoundsException e) {
			namespace = null;
		}

		String name;
		try {
			name = (fragment != null && !fragment.isEmpty()) ? fragment
					.substring(fragment.lastIndexOf(".") + 1, fragment.length())
					: "";

			if (namespace != null) {
				sql += "OR ";

			}
			sql += " et.name LIKE '%" + name + "%';";
		} catch (StringIndexOutOfBoundsException e) {
			name = fragment;
		}

		List<EntityType> entities = null;
		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			// ps.setString(1, "%" + fragment + "%");
			// ps.setString(2, "%" + fragment + "%");

			ResultSet resultSet = ps.executeQuery();
			entities = new ArrayList<EntityType>();
			while (resultSet.next()) {
				EntityType entity = new EntityType();
				entity.setId(resultSet.getLong("id"));
				entity.setVersion(resultSet.getInt("version"));
				entity.setNamespace(resultSet.getString("namespace"));
				entity.setName(resultSet.getString("name"));
				entities.add(entity);
			}
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return entities;
	}

	public EntityType findByFullName(String fullName) {
		List<EntityType> entities = this.listByFullName(fullName);

		if (entities != null && entities.size() > 0) {
			return entities.get(0);
		}
		return null;
	}

	public EntityType update(EntityType entity) {
		String sql = "UPDATE " + getDatabaseName() + "." + getNameTable()
				+ " SET version = ?, namespace = ?, name = ? where id = ?;";

		try {
			Connection connection = this.createConnection();

			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, entity.getVersion() + 1);
			ps.setString(2, entity.getNamespace());
			ps.setString(3, entity.getName());
			ps.setLong(4, entity.getId());
			ps.executeUpdate();
			this.closeConexao();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return findById(entity.getId());
	}

	public void delete(Long id) {
		String sql = "DELETE FROM " + getDatabaseName() + "." + getNameTable()
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

	@Override
	public String getNameTable() {
		return "entityType";
	}
}
