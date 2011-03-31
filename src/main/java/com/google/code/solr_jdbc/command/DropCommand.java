package com.google.code.solr_jdbc.command;

import net.sf.jsqlparser.statement.drop.Drop;

import com.google.code.solr_jdbc.impl.AbstractResultSet;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;


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
	public AbstractResultSet executeQuery() {
		throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
	}

	@Override
	public int executeUpdate() {
		try {
			conn.getSolrServer().deleteByQuery(
					"meta.name:"+statement.getName()+" OR id:@"+statement.getName()+".*");
			conn.setUpdatedInTx(true);
			conn.commit();
		} catch(Exception e) {
			DbException.get(ErrorCode.GENERAL_ERROR, e, "Solr Server Error");
		}
		return 0;
	}

}
