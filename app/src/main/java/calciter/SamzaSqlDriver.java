package calciter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.Driver;


/**
 * Calcite JDBC driver for SamzaSQL which takes in a {@link JavaTypeFactory}
 */
public class SamzaSqlDriver extends Driver {

  private JavaTypeFactory typeFactory;

  public SamzaSqlDriver(JavaTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    final String prefix = getConnectStringPrefix();
    assert url.startsWith(prefix);
    final String urlSuffix = url.substring(prefix.length());
    final Properties info2 = ConnectStringParser.parse(urlSuffix, info);
    final AvaticaConnection connection =
        ((CalciteFactory) factory).newConnection(this, factory, url, info2, null, typeFactory);
    handler.onConnectionInit(connection);
    return connection;
  }
}
