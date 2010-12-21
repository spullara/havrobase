package avrobase.hs;

import avrobase.AvroBaseException;
import avrobase.AvroFormat;
import avrobase.Row;
import avrobase.mysql.KeyStrategy;
import avrobase.mysql.MysqlAB;
import com.google.code.hs4j.HSClient;
import com.google.code.hs4j.IndexSession;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.concurrent.ExecutorService;

/**
 * HandlerSocket AvroBase is just a thin layer on top of Mysql with different access pattern for the
 * main lookup and scan cases.
 * <p/>
 * User: sam
 * Date: 12/20/10
 * Time: 12:25 PM
 */
public class HSAB<T extends SpecificRecord, K> extends MysqlAB<T, K> {
  private IndexSession session;

  @Inject
  public HSAB(ExecutorService es, DataSource datasource, HSClient hsClient, String table, String family,
              String schemaTable, Schema schema, AvroFormat storageFormat, KeyStrategy<K> keytx) throws AvroBaseException {
    super(es, datasource, table, family, schemaTable, schema, storageFormat, keytx);
    try {
      String url = datasource.getConnection().getMetaData().getURL();
      String database = url.substring(url.lastIndexOf("/") + 1);
      session = hsClient.openIndexSession(database, mysqlTableName, "PRIMARY", new String[]{"schema_id", "version", "format", "avro"});
    } catch (Exception e) {
      throw new AvroBaseException("Failed to open index", e);
    }
  }

  @Override
  public Row<T, K> get(byte[] row) throws AvroBaseException {
    try {
      ResultSet rs = session.find(new String[]{new String(row)});
      if (rs.next()) {
        int schema_id = rs.getInt(1);
        long version = rs.getLong(2);
        AvroFormat format = AvroFormat.values()[rs.getByte(3)];
        byte[] avro = rs.getBytes(4);
        Schema schema = getSchema(schema_id);
        if (schema != null) {
          return new Row<T, K>(readValue(avro, schema, format), keytx.fromBytes(row), version);
        } else {
          throw new AvroBaseException("Failed to find schema: " + schema_id);
        }
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new AvroBaseException("Failed to retrieve row", e);
    }
  }
}
