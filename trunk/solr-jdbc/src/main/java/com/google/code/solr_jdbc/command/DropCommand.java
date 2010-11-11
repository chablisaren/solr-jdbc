package com.google.code.solr_jdbc.command;

import java.sql.ResultSet;

import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;

import net.sf.jsqlparser.statement.drop.Drop;


public class DropCommand extends Command {
	private Drop statement;

	protected DropCommand(Drop statement) {
		this.statement = statement;
	}
	
	@Override
	public boolean isQuery() {
		return false;
	}
	
	@Override
	public void parse() {
		
	}
	
	@Override
	public ResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@Override
	public int executeUpdate() {
		try {
			conn.getSolrServer().deleteByQuery(
					"meta.name:"+statement.getName()+" OR id:@"+statement.getName()+".*");
			conn.commit();
		} catch(Exception e) {
			DbException.get(ErrorCode.GENERAL_ERROR, e);
		}
		return 0;
	}

}
