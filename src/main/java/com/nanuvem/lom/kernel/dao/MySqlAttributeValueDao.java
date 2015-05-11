package com.nanuvem.lom.kernel.dao;

import com.nanuvem.lom.api.AttributeValue;
import com.nanuvem.lom.api.dao.AttributeValueDao;

public class MySqlAttributeValueDao extends AbstractRelationalDAO implements
		AttributeValueDao {

	public MySqlAttributeValueDao(MySqlConnectionFactory connectionFactory) {
		super(connectionFactory);
		// connectionFactory.setDatabaseName("lom");
	}

	public AttributeValue create(AttributeValue value) {
		// TODO Auto-generated method stub
		return null;
	}

	public AttributeValue update(AttributeValue value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNameTable() {
		return "attribute";
	}

}