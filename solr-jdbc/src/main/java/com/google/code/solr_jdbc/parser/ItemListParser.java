package com.google.code.solr_jdbc.parser;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;

import com.google.code.solr_jdbc.expression.Item;
import com.google.code.solr_jdbc.expression.Literal;
import com.google.code.solr_jdbc.expression.Parameter;
import com.google.code.solr_jdbc.message.DbException;
import com.google.code.solr_jdbc.message.ErrorCode;

public class ItemListParser implements ItemsListVisitor{
	private List<Parameter> parameters;
	private List<Item> itemList;

	public ItemListParser() {
		this.itemList = new ArrayList<Item>();
		this.parameters = new ArrayList<Parameter>();
	}
	
	public ItemListParser(List<Parameter> parameters) {
		this();
		this.parameters.addAll(parameters);
	}
	
	@Override
	public void visit(SubSelect subselect) {
		throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED, "SubQuery");
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void visit(ExpressionList expressionList) {
		ExpressionParser expressionParser = new ExpressionParser();
		for(Expression expression : (List<Expression>)expressionList.getExpressions()) {
			expression.accept(expressionParser);
			if (expressionParser.isParameter()) {
				Parameter p = new Parameter(parameters.size());
				parameters.add(p);
				itemList.add(p);
			} else {
				itemList.add(new Literal(expressionParser.getValue()));
			}
		}
		
	}

	public List<Parameter> getParameters() {
		return parameters;
	}

	public List<Item> getItemList() {
		return itemList;
	}
}
