package avrobase.mysql;

import avrobase.*;
import com.google.inject.Inject;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mysql backed implementation of Avrobase.
 * <p/>
 * User: sam, john
 * Date: Jun 18, 2010
 * Time: 1:59:33 PM
 * TODO: consider column-type-specific support (via keytx)
 */
public class MysqlAB<T extends SpecificRecord, K, Q> extends AvroBaseImpl<T, K, Q> implements AvroBase<T, K, Q> {
  private final DataSource datasource;
  private final AvroFormat storageFormat;
  private final String mysqlTableName;
  private final String schemaTable;
  private final KeyStrategy<K> keytx;

  // Caches
  private Map<Integer, Schema> abbrevSchema = new ConcurrentHashMap<Integer, Schema>();
  private Map<String, Schema> lookupSchema = new ConcurrentHashMap<String, Schema>();
  private Map<Schema, Integer> schemaAbbrev = new ConcurrentHashMap<Schema, Integer>();

  @Inject
  public MysqlAB(
      DataSource datasource,
      String table,
      String family,
      String schemaTable,
      Schema schema,
      AvroFormat storageFormat,
      KeyStrategy<K> keytx) throws AvroBaseException {

    super(schema, storageFormat);
    this.datasource = datasource;
    this.schemaTable = schemaTable;
    this.storageFormat = storageFormat;
    this.mysqlTableName = table + "__" + family;
    this.keytx = keytx;

    try {
      // TODO: turn this 
      Connection connection = null;
      try {
        connection = datasource.getConnection();
        DatabaseMetaData data = connection.getMetaData();
        {
          ResultSet tables = data.getTables(null, null, mysqlTableName, null);
          if (!tables.next()) {
            // Create the table
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE " + mysqlTableName + " ( row varbinary(256) primary key, schema_id integer not null, version integer not null, format tinyint not null, avro mediumblob not null )");
            statement.close();
          }
          tables.close();
        }
        {
          ResultSet tables = data.getTables(null, null, this.schemaTable, null);
          if (!tables.next()) {
            // Create the table
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE " + this.schemaTable + " ( id integer primary key auto_increment, hash varbinary(256) not null, json longblob not null )");
            statement.close();
          } else {
            // Load schemas
            new Query<Void>("SELECT id, hash, json FROM " + MysqlAB.this.schemaTable) {
              void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
              }

              Void execute(ResultSet rs) throws AvroBaseException, SQLException {
                while (rs.next()) {
                  int id = rs.getInt(1);
                  String hash = new String(rs.getBytes(2));
                  loadSchema(id, hash, rs.getBytes(3));
                }
                return null;
              }
            }.query();
          }
          tables.close();
        }
      } finally {
        if (connection != null) connection.close();
      }
    } catch (SQLException sqle) {
      throw new AvroBaseException("Problem with MySQL", sqle);
    }
  }

  private int storeSchema(final Schema schema) throws AvroBaseException {
    Integer id;
    synchronized (schema) {
      id = schemaAbbrev.get(schema);
      if (id == null) {
        // Hash the schema, store it
        MessageDigest md;
        try {
          md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
          md = null;
        }
        String doc = schema.toString();
        final String schemaKey;
        if (md == null) {
          schemaKey = doc;
        } else {
          schemaKey = new String(Hex.encodeHex(md.digest(doc.getBytes())));
        }
        id = new Query<Integer>("SELECT id FROM " + schemaTable + " WHERE hash=?") {
          void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
            ps.setBytes(1, schemaKey.getBytes());
          }

          Integer execute(ResultSet rs) throws AvroBaseException, SQLException {
            if (rs.next()) {
              return rs.getInt(1);
            } else {
              return null;
            }
          }
        }.query();
        if (id == null) {
          id = new Insert("INSERT INTO " + schemaTable + " (hash, json) VALUES (?, ?)") {
            void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
              ps.setBytes(1, schemaKey.getBytes());
              ps.setBytes(2, schema.toString().getBytes());
            }
          }.insert();
        }
        abbrevSchema.put(id, schema);
        lookupSchema.put(schemaKey, schema);
        schemaAbbrev.put(schema, id);
      }
    }
    return id;
  }

  @Override
  public Row<T, K> get(K row) throws AvroBaseException {
    return get(keytx.toBytes(row));
  }

  @Override
  public K create(T value) throws AvroBaseException {
    final K key = keytx.newKey();
    if (!put(key, value, 0)) {
      throw new AvroBaseException("did not add " + key);
    } else {
      return key;
    }
  }

  @Override
  public void put(K row, T value) throws AvroBaseException {
    put(keytx.toBytes(row), value);
  }

  @Override
  public boolean put(K row, T value, long version) throws AvroBaseException {
    return put(keytx.toBytes(row), value, version);
  }

  @Override
  public void delete(K row) throws AvroBaseException {
    throw new NotImplementedException();
    /// TODO delete(keytx.toBytes(row));
  }

  @Override
  public Iterable<Row<T, K>> scan(K startRow, K stopRow) throws AvroBaseException {
    return scan(startRow != null ? keytx.toBytes(startRow) : null, stopRow != null ? keytx.toBytes(stopRow) : null);
  }

  @Override
  public Iterable<Row<T, K>> search(Q query) throws AvroBaseException {
    throw new NotImplementedException();
  }

  private abstract class Update {
    private String statement;

    Update(String statement) {
      this.statement = statement;
    }

    abstract void setup(PreparedStatement ps) throws AvroBaseException, SQLException;

    int insert() throws AvroBaseException {
      try {
        Connection c = null;
        PreparedStatement ps = null;
        try {
          c = datasource.getConnection();
          ps = c.prepareStatement(statement);
          setup(ps);
          return ps.executeUpdate();
        } finally {
          if (ps != null) ps.close();
          if (c != null) c.close();
        }
      } catch (SQLException e) {
        throw new AvroBaseException("Database problem", e);
      }
    }
  }

  private abstract class Insert {
    private String statement;

    Insert(String statement) {
      this.statement = statement;
    }

    abstract void setup(PreparedStatement ps) throws AvroBaseException, SQLException;

    int insert() throws AvroBaseException {
      try {
        Connection c = null;
        PreparedStatement ps = null;
        PreparedStatement ps2 = null;
        ResultSet rs2 = null;
        try {
          c = datasource.getConnection();
          ps = c.prepareStatement(statement);
          setup(ps);
          int rows = ps.executeUpdate();
          if (rows != 1) {
            throw new AvroBaseException("inserted wrong number of rows: " + rows);
          }
          ps2 = c.prepareStatement("SELECT LAST_INSERT_ID()");
          rs2 = ps2.executeQuery();
          if (rs2.next()) {
            return rs2.getInt(1);
          } else {
            throw new AvroBaseException("unexpected response");
          }
        } finally {
          if (rs2 != null) ps.close();
          if (ps2 != null) ps.close();
          if (ps != null) ps.close();
          if (c != null) c.close();
        }
      } catch (SQLException e) {
        throw new AvroBaseException("Database problem", e);
      }
    }
  }


  private abstract class Query<R> {
    private String statement;

    Query(String statement) {
      this.statement = statement;
    }

    abstract void setup(PreparedStatement ps) throws AvroBaseException, SQLException;

    abstract R execute(ResultSet rs) throws AvroBaseException, SQLException;

    R query() throws AvroBaseException {
      try {
        Connection c = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
          c = datasource.getConnection();
          ps = c.prepareStatement(statement);
          setup(ps);
          rs = ps.executeQuery();
          return execute(rs);
        } finally {
          if (rs != null) rs.close();
          if (ps != null) ps.close();
          if (c != null) c.close();
        }
      } catch (SQLException e) {
        throw new AvroBaseException("Database problem", e);
      }
    }
  }

  public Row<T, K> get(final byte[] row) throws AvroBaseException {
    return new Query<Row<T, K>>("SELECT schema_id, version, format, avro FROM " + mysqlTableName + " WHERE row=?") {
      void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        ps.setBytes(1, row);
      }

      Row<T, K> execute(ResultSet rs) throws AvroBaseException, SQLException {
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
      }
    }.query();
  }

  @Override
  protected K $(String key) {
    return keytx.fromString(key);
  }

  @Override
  protected String $_(K key) {
    return keytx.toString(key);
  }

  private synchronized Schema getSchema(final int schema_id) throws AvroBaseException {
    Schema schema = abbrevSchema.get(schema_id);
    if (schema == null) {
      schema = new Query<Schema>("SELECT id, hash, json FROM " + schemaTable + " WHERE id=?") {
        void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
          ps.setInt(1, schema_id);
        }

        Schema execute(ResultSet rs) throws AvroBaseException, SQLException {
          if (rs.next()) {
            String hash = new String(rs.getBytes(2));
            return loadSchema(schema_id, hash, rs.getBytes(3));
          } else {
            return null;
          }
        }
      }.query();
    }
    return schema;
  }

  private Schema loadSchema(int id, String hash, byte[] value) throws AvroBaseException {
    Schema schema;
    try {
      schema = Schema.parse(new ByteArrayInputStream(value));
    } catch (IOException e) {
      throw new AvroBaseException("Could not parse the schema", e);
    }
    abbrevSchema.put(id, schema);
    lookupSchema.put(hash, schema);
    schemaAbbrev.put(schema, id);
    return schema;
  }

  public void put(final byte[] row, final T value) throws AvroBaseException {
    Schema schema = value.getSchema();
    Integer schemaId = schemaAbbrev.get(schema);
    if (schemaId == null) {
      schemaId = storeSchema(schema);
    }
    final Integer finalSchemaId = schemaId;
    int updated = new Update("INSERT INTO " + mysqlTableName + " (row, schema_id, version, format, avro) VALUES (?,?,1,?,?) " +
        "ON DUPLICATE KEY UPDATE schema_id=values(schema_id), version = version + 1, format=values(format), avro=values(avro)") {
      void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        ps.setBytes(1, row);
        ps.setInt(2, finalSchemaId);
        ps.setInt(3, storageFormat.ordinal());
        ps.setBytes(4, serialize(value));
      }
    }.insert();
    if (updated == 0) {
      throw new AvroBaseException("Failed to save: " + updated);
    }
  }

  public boolean put(final byte[] row, final T value, final long version) throws AvroBaseException {
    Schema schema = value.getSchema();
    Integer schemaId = schemaAbbrev.get(schema);
    if (schemaId == null) {
      schemaId = storeSchema(schema);
    }
    final Integer finalSchemaId = schemaId;
    if (version == 0) {
      try {
        int updated = new Update("INSERT INTO " + mysqlTableName + " (row, schema_id, version, format, avro) VALUES (?,?," +
            "1,?,?)") {
          void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
            ps.setBytes(1, row);
            ps.setInt(2, finalSchemaId);
            ps.setInt(3, storageFormat.ordinal());
            ps.setBytes(4, serialize(value));
          }
        }.insert();
        if (updated == 0) {
          return false;
        }
      } catch (AvroBaseException e) {
        if (e.getCause() instanceof SQLException) return false;
        throw e;
      }
    } else {
      int updated = new Update("UPDATE " + mysqlTableName + " SET schema_id=?, version = version + 1, format=?, avro=? WHERE row=? AND version = ?") {
        void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
          ps.setInt(1, finalSchemaId);
          ps.setInt(2, storageFormat.ordinal());
          ps.setBytes(3, serialize(value));
          ps.setBytes(4, row);
          ps.setLong(5, version);
        }
      }.insert();
      if (updated == 0) {
        return false;
      }
    }
    return true;
  }

  public Iterable<Row<T, K>> scan(final byte[] startRow, final byte[] stopRow) throws AvroBaseException {
    StringBuilder statement = new StringBuilder("SELECT row, schema_id, version, format, avro FROM ");
    statement.append(mysqlTableName);
    if (startRow != null) {
      statement.append(" WHERE row >= ?");
    }
    if (stopRow != null) {
      if (startRow == null) {
        statement.append(" WHERE row < ?");
      } else {
        statement.append(" AND row < ?");
      }
    }
    return new Query<Iterable<Row<T, K>>>(statement.toString()) {
      void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        int i = 1;
        if (startRow != null) {
          ps.setBytes(i++, startRow);
        }
        if (stopRow != null) {
          ps.setBytes(i, stopRow);
        }
      }

      Iterable<Row<T, K>> execute(final ResultSet rs) throws AvroBaseException, SQLException {
        // TODO: Can't stream this yet due to database cursors
        List<Row<T, K>> rows = new ArrayList<Row<T, K>>();
        while (rs.next()) {
          byte[] row = rs.getBytes(1);
          int schema_id = rs.getInt(2);
          long version = rs.getLong(3);
          AvroFormat format = AvroFormat.values()[rs.getByte(4)];
          byte[] avro = rs.getBytes(5);
          Schema schema = getSchema(schema_id);
          if (schema != null) {
            rows.add(new Row<T, K>(readValue(avro, schema, format), keytx.fromBytes(row), version));
          } else {
            // TODO: logging
            System.err.println("skipped row because of missing schema: " + keytx.fromBytes(row) + " schema " + schema_id);
          }
        }
        return rows;
      }
    }.query();
  }
}
