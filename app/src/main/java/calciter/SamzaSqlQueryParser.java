package calciter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;


/**
 * Utility class that is used to parse the Samza sql query to figure out the sources, sink etc..
 */
public class SamzaSqlQueryParser {
  private static final String TRAILING_SEMI_COLON_REGEX = ";+$";

  private SamzaSqlQueryParser() {
  }

  public static class QueryInfo {
    private final List<String> sources;
    private String selectQuery;
    private String sink;
    private String sql;

    public QueryInfo(String selectQuery, List<String> sources, String sink, String sql) {
      this.selectQuery = selectQuery;
      this.sink = sink;
      this.sources = sources;
      this.sql = sql;
    }

    public List<String> getSources() {
      return sources;
    }

    public String getSelectQuery() {
      return selectQuery;
    }

    public String getSink() {
      return sink;
    }

    public String getSql() {
      return sql;
    }
  }

  /**
   *
   *    example about columns:
   *
        SqlCreateTable create = (SqlCreateTable) sqlNode;
        final SqlColumnDeclaration sqlColumnDeclaration = (SqlColumnDeclaration) create.columnList
                .get(0);
        Assert.assertEquals("X", sqlColumnDeclaration.name.getSimple());
        Assert.assertEquals("INTEGER",
                sqlColumnDeclaration.dataType.getTypeName().getSimple());
   *
   *
   * */
  public static QueryInfo parseQuery(String sql) {
    // Having semi-colons at the end of sql statement is a valid syntax in standard sql but not for Calcite parser.
    // Hence, removing trailing semi-colon before passing sql statement to Calcite parser.
    sql = sql.replaceAll(TRAILING_SEMI_COLON_REGEX, "");
    SqlNode sqlNode;

    Config cfg = SqlParser.Config.DEFAULT
            .withLex(Lex.MYSQL_ANSI).withConformance(SqlConformanceEnum.MYSQL_5)
            .withParserFactory(SqlDdlParserImpl.FACTORY);

    try {
      sqlNode = SqlParser.create(sql, cfg).parseStmt();
    } catch (SqlParseException e) {
      throw new SamzaException("failed to parse sql", e);
    }

    String sink = "";
    String selectQuery = "";
    ArrayList<String> sources = null;
    if (sqlNode instanceof SqlInsert) {
      SqlInsert sqlInsert = (SqlInsert) sqlNode;
      sink = sqlInsert.getTargetTable().toString();
      if (sqlInsert.getSource() instanceof SqlSelect) {
        SqlSelect sqlSelect = (SqlSelect) sqlInsert.getSource();
        selectQuery = sqlSelect.toString();
        sources = getSourcesFromSelectQuery(sqlSelect);
      } else {
        String msg = String.format("Sql query is not of the expected format. Select node expected, found %s",
            sqlInsert.getSource().getClass().toString());
        throw new SamzaException(msg);
      }
    } else if (sqlNode instanceof SqlCreateTable) {
      SqlCreateTable sqlCreate = (SqlCreateTable) sqlNode;
      sink = sqlCreate.name.getSimple();
      // CREATE TABLE .. AS 
      if (sqlCreate.query != null && sqlCreate.query instanceof SqlSelect) {
        SqlSelect sqlSelect = (SqlSelect) sqlCreate.query;
        selectQuery = sqlSelect.toString();
        sources = getSourcesFromSelectQuery(sqlSelect);
      }
    } else {
      String msg = String.format("Sql query is not of the expected format. Insert node expected, found %s",
          sqlNode.getClass().toString());
      throw new SamzaException(msg);
    }

    return new QueryInfo(selectQuery, sources, sink, sql);
  }

  //private static Planner createPlanner() {
  //  Connection connection;
  //  SchemaPlus rootSchema;
  //  try {
  //    JavaTypeFactory typeFactory = new SamzaSqlJavaTypeFactoryImpl();
  //    SamzaSqlDriver driver = new SamzaSqlDriver(typeFactory);
  //    DriverManager.deregisterDriver(DriverManager.getDriver("jdbc:calcite:"));
  //    DriverManager.registerDriver(driver);
  //    connection = driver.connect("jdbc:calcite:", new Properties());
  //    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
  //    rootSchema = calciteConnection.getRootSchema();
  //  } catch (SQLException e) {
  //    throw new SamzaException(e);
  //  }

  //  final List<RelTraitDef> traitDefs = new ArrayList<>();

  //  traitDefs.add(ConventionTraitDef.INSTANCE);
  //  traitDefs.add(RelCollationTraitDef.INSTANCE);

  //  FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
  //      .parserConfig(SqlParser.configBuilder().setLex(Lex.JAVA).build())
  //      .defaultSchema(rootSchema)
  //      .operatorTable(SqlStdOperatorTable.instance())
  //      .traitDefs(traitDefs)
  //      .context(Contexts.EMPTY_CONTEXT)
  //      .costFactory(null)
  //      //.programs(Programs.CALC_PROGRAM)
  //      .build();
  //  return Frameworks.getPlanner(frameworkConfig);
  //}

  private static ArrayList<String> getSourcesFromSelectQuery(SqlSelect sqlSelect) {
    ArrayList<String> sources = new ArrayList<>();
    getSource(sqlSelect.getFrom(), sources);
    if (sources.size() < 1) {
      throw new SamzaException("Unsupported query " + sqlSelect);
    }

    return sources;
  }

  private static void getSource(SqlNode node, ArrayList<String> sourceList) {
    if (node instanceof SqlJoin) {
      SqlJoin joinNode = (SqlJoin) node;
      ArrayList<String> sourcesLeft = new ArrayList<>();
      ArrayList<String> sourcesRight = new ArrayList<>();
      getSource(joinNode.getLeft(), sourcesLeft);
      getSource(joinNode.getRight(), sourcesRight);

      sourceList.addAll(sourcesLeft);
      sourceList.addAll(sourcesRight);
    } else if (node instanceof SqlIdentifier) {
      sourceList.add(node.toString());
    } else if (node instanceof SqlBasicCall) {
      SqlBasicCall basicCall = (SqlBasicCall) node;
      if (basicCall.getOperator() instanceof SqlAsOperator) {
        getSource(basicCall.operand(0), sourceList);
      } else if (basicCall.getOperator() instanceof SqlUnnestOperator && basicCall.operand(0) instanceof SqlSelect) {
        sourceList.addAll(getSourcesFromSelectQuery(basicCall.operand(0)));
      }
    } else if (node instanceof SqlSelect) {
      getSource(((SqlSelect) node).getFrom(), sourceList);
    }
  }
}
