package solr.jdbc.value;

import java.math.BigDecimal;

public class ValueNull extends SolrValue {
	public static final ValueNull INSTANCE = new ValueNull();

	public static final ValueNull DELETED = new ValueNull();

	private ValueNull() {

	}

	@Override
	public BigDecimal getBigDecimal() {
		return null;
	}
	
	public int getInt() {
		return 0;
	}
	
	@Override
	public SolrType getType() {
		return SolrType.NULL;
	}
	@Override
	public String getQueryString() {
		return null;
	}

	@Override
	public String getString() {
		return null;
	}

	@Override
	public Object getObject() {
		return null;
	}

}
