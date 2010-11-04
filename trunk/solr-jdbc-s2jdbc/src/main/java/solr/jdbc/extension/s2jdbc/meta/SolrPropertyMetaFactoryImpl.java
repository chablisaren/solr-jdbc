package solr.jdbc.extension.s2jdbc.meta;

import java.util.List;

import org.seasar.extension.jdbc.EntityMeta;
import org.seasar.extension.jdbc.PropertyMeta;
import org.seasar.extension.jdbc.PropertyMetaFactory;
import org.seasar.extension.jdbc.ValueType;
import org.seasar.extension.jdbc.meta.PropertyMetaFactoryImpl;

import solr.jdbc.extension.s2jdbc.types.ArrayType;
import solr.jdbc.extension.s2jdbc.types.ListType;

public class SolrPropertyMetaFactoryImpl extends PropertyMetaFactoryImpl {
	public final static ValueType ARRAY = new ArrayType(); 
	public final static ValueType LIST  = new ListType(); 
		
	@Override
	protected void doValueType(final PropertyMeta propertyMeta,
			final EntityMeta entityMeta) {
		final Class<?> propertyClass = propertyMeta.getPropertyClass();
		
		if(propertyClass.isArray()) {
			propertyMeta.setValueType(ARRAY);
			return;
		}
		if(List.class.isAssignableFrom(propertyClass)) {
			propertyMeta.setValueType(LIST);
			return;
		}
		super.doValueType(propertyMeta, entityMeta);
	}
	
}
