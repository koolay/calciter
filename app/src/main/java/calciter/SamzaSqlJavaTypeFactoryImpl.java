package calciter;

import com.google.common.collect.Lists;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.JavaToSqlTypeConversionRules;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * Calcite does validation of projected field types in select statement with the output schema types. If one of the
 * projected fields is an UDF with return type of {@link Object} or any other java type not defined in
 * {@link JavaToSqlTypeConversionRules}, using the default {@link JavaTypeFactoryImpl} results in validation failure.
 * Hence, extending {@link JavaTypeFactoryImpl} to make Calcite validation work with all output types of Samza SQL UDFs.
 */
public class SamzaSqlJavaTypeFactoryImpl
    extends JavaTypeFactoryImpl {

  public SamzaSqlJavaTypeFactoryImpl() {
    this(RelDataTypeSystem.DEFAULT);
  }

  public SamzaSqlJavaTypeFactoryImpl(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  @Override
  public RelDataType toSql(RelDataType type) {
    return convertToSql(this, type);
  }

  // Converts a type in Java format to a SQL-oriented type.
  private static RelDataType convertToSql(final RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type instanceof RelRecordType) {
      return typeFactory.createStructType(
          Lists.transform(type.getFieldList(), a0 -> convertToSql(typeFactory, a0.getType())),
          type.getFieldNames());
    }
    if (type instanceof JavaType) {
      SqlTypeName typeName = JavaToSqlTypeConversionRules.instance().lookup(((JavaType) type).getJavaClass());
      // For unknown sql type names, return ANY sql type to make Calcite validation not fail.
      if (typeName == null) {
        typeName = SqlTypeName.ANY;
      }
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(typeName),
          type.isNullable());
    } else {
      return JavaTypeFactoryImpl.toSql(typeFactory, type);
    }
  }
}
