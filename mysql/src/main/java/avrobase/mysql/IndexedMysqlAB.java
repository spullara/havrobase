package avrobase.mysql;

import avrobase.AvroBaseException;
import avrobase.AvroBaseImpl;
import avrobase.AvroFormat;
import avrobase.Row;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.NotImplementedException;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO: fix created/updated columns
 * @param <T>
 */
public class IndexedMysqlAB<T extends SpecificRecord> extends AvroBaseImpl<T, Long> {
  private static final String[] baseCols = new String[]{"id", "version", "created", "updated", "schema_id", "avro"};
  private static final AvroFormat avroFormat = AvroFormat.JSON;

  private static final String[] baseColDefs = new String[]{
      "id bigint not null primary key",
      "version integer not null",
      "created timestamp not null",
      "updated timestamp not null",
  };

  private static final String[] dataColDefs = new String[]{
    "schema_id integer not null",
    "avro mediumblob not null"
  };

  private final DataSource datasource;
  private final String schemaTable;
  private final String table;
  private final Iterable<IndexColumn<T, ?>> indexColumns;
  private final ManyManyRelation<T> relation; // may be null

  // pre-computed SQL statements
  private final String versionedUpdateStatement;
  private final String upsertStatement;
  private final String selectStatement;
  private final String insertStatement;

  // Caches
  private final Map<Integer, Schema> abbrevSchema = new ConcurrentHashMap<Integer, Schema>();
  private final Map<Schema, Integer> schemaAbbrev = new ConcurrentHashMap<Schema, Integer>();

  public IndexedMysqlAB(DataSource datasource, String table, Schema schema, String schemaTable, Iterable<IndexColumn<T, ?>> indexColumns, @Nullable ManyManyRelation<T> relation) {
    super(schema, avroFormat);

    this.datasource = checkNotNull(datasource);
    this.table = checkNotNull(table);
    this.schemaTable = checkNotNull(schemaTable);
    this.indexColumns = checkNotNull(indexColumns);
    this.relation = relation;

    final List<String> ddlClauses = new LinkedList<String>();

    for (String col : baseColDefs) {
      ddlClauses.add(col);
    }

    if (relation != null) {
      ddlClauses.add(relation.getLeftColName() + " bigint not null");
      ddlClauses.add(relation.getRightColName() + " bigint not null");
    }

    for (IndexColumn<T, ?> ic : indexColumns) {
      ddlClauses.add(ic.columnName() + " " + ic.columnTypeString() + (ic.nullable() ? " not null" : ""));
    }

    for (String col : dataColDefs) {
      ddlClauses.add(col);
    }

    if (relation != null) {
      ddlClauses.add("UNIQUE INDEX (" + relation.getLeftColName() + "," + relation.getRightColName() + ")");
      ddlClauses.add("INDEX (" + relation.getRightColName() + ")");
    }

    for (IndexColumn<T, ?> ic : indexColumns) {
      ddlClauses.add((ic.unique() ? "UNIQUE " : "") + "INDEX (" + ic.columnName() + ")");
    }

    createTable(ddlClauses);

    final String insertColsClause = columnClause(createInsertCols(relation, indexColumns));
    final String insertParamClause = paramClause(baseCols.length + Iterables.size(indexColumns) + (relation != null ? 2 : 0));
    versionedUpdateStatement = "UPDATE " + table + " SET schema_id=?, version=version+1, avro=? WHERE id=? AND version=?";
    upsertStatement = "INSERT INTO " + table + insertColsClause + " VALUES " + insertParamClause + " ON DUPLICATE KEY UPDATE schema_id=values(schema_id), version = version + 1, avro=values(avro), updated=values(updated)";
    selectStatement = "SELECT schema_id, version, avro FROM " + table + " WHERE id=?";
    insertStatement = "INSERT INTO " + table + " " + insertColsClause + " VALUES " + insertParamClause;
  }

  public void createTable(List<String> ddlClauses) {
    try {
      // TODO: turn this
      Connection connection = null;
      try {
        connection = datasource.getConnection();
        DatabaseMetaData data = connection.getMetaData();
        {
          ResultSet tables = data.getTables(null, null, table, null);
          if (!tables.next()) {
            // Create the table
            Statement statement = connection.createStatement();
            String createString = "CREATE TABLE " + table + columnClause(ddlClauses);
            System.out.println(createString);
            statement.executeUpdate(createString);
            statement.close();
          }
          tables.close();
        }
        {
          ResultSet tables = data.getTables(null, null, this.schemaTable, null);
          if (!tables.next()) {
            // Create the table
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE " + this.schemaTable + " (id integer primary key auto_increment, json longblob not null)");
            statement.close();
          } else {
            // Load schemas
            new Query<Void>("SELECT id, json FROM " + IndexedMysqlAB.this.schemaTable) {
              void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
              }

              Void execute(ResultSet rs) throws AvroBaseException, SQLException {
                while (rs.next()) {
                  loadSchema(rs.getInt(1), rs.getBytes(2));
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

  @Override
  public Row<T, Long> get(final Long row) throws AvroBaseException {
    return new Query<Row<T, Long>>(selectStatement) {
      void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        ps.setLong(1, row);
      }

      Row<T, Long> execute(ResultSet rs) throws AvroBaseException, SQLException {
        if (rs.next()) {
          int schema_id = rs.getInt(1);
          long version = rs.getLong(2);
          byte[] avro = rs.getBytes(3);
          Schema schema = getSchema(schema_id);
          if (schema != null) {
            return new Row<T, Long>(readValue(avro, schema, format), row, version);
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
  public Long create(T value) throws AvroBaseException {
    Random r = new Random();
    final long key = r.nextLong();
    if (!put(key, value, 0)) {
      throw new AvroBaseException("did not add " + key);
    } else {
      return key;
    }
  }

  @Override
  public void put(final Long id, final T value) throws AvroBaseException {
    Schema schema = value.getSchema();
    Integer schemaId = schemaAbbrev.get(schema);
    if (schemaId == null) {
      schemaId = storeSchema(schema);
    }
    final Integer finalSchemaId = schemaId;
    System.out.println(upsertStatement);
    int updated = new Update(upsertStatement) {
      void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        parameterize(ps, id, 1, finalSchemaId, value, System.currentTimeMillis());
      }
    }.insert();
    if (updated == 0) {
      throw new AvroBaseException("Failed to save: " + updated);
    }
  }

  private List<String> createInsertCols(ManyManyRelation relation, Iterable<IndexColumn<T, ?>> idxColumns) {
    // Order: base cols, relation cols, other index cols
    List<String> cols = new ArrayList<String>();
    for (String col : baseCols) {
      cols.add(col);
    }
    if (relation != null) {
      cols.add(relation.getLeftColName());
      cols.add(relation.getRightColName());
    }
    for (IndexColumn<?, ?> idx : idxColumns) {
      cols.add(idx.columnName());
    }
    return cols;
  }

  @Override
  public boolean put(final Long id, final T value, final long version) throws AvroBaseException {
    Schema schema = value.getSchema();
    Integer schemaId = schemaAbbrev.get(schema);
    if (schemaId == null) {
      schemaId = storeSchema(schema);
    }
    final Integer finalSchemaId = schemaId;
    if (version == 0) {
      try {
        System.out.println(insertStatement);
        int updated = new Update(insertStatement) { // TODO: cache
          void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
            parameterize(ps, id, 1, finalSchemaId, value, System.currentTimeMillis());
          }
        }.insert();
        if (updated == 0) {
          return false;
        }
      } catch (AvroBaseException e) {
        if (!(e.getCause() instanceof SQLException)) return false;
        throw e;
      }
    } else {
      int updated = new Update(versionedUpdateStatement) {
        void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
          ps.setInt(1, finalSchemaId);
          ps.setBytes(2, serialize(value));
          ps.setLong(3, id);
          ps.setLong(4, version);
        }
      }.insert();
      if (updated == 0) {
        return false;
      }
    }
    return true;
  }

  private void parameterize(PreparedStatement ps, Long id, int version, Integer schemaId, T value, long timestamp) throws SQLException {
    int i = 1;
    ps.setLong(i++, id);
    ps.setInt(i++, version);
    ps.setTimestamp(i++, new Timestamp(timestamp));
    ps.setTimestamp(i++, new Timestamp(timestamp));
    ps.setInt(i++, schemaId);
    ps.setBytes(i++, serialize(value));

    if (relation != null) {
      ps.setLong(i++, relation.getLeftFn().apply(value));
      ps.setLong(i++, relation.getRightFn().apply(value));
    }

    for (IndexColumn<T, ?> ic : indexColumns) {
      ps.setObject(i++, ic.extractKeyFn().apply(value), ic.columnType());
    }
  }

  @Override
  public void delete(Long row) throws AvroBaseException {
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Row<T, Long>> scan(Long startRow, Long stopRow) throws AvroBaseException {
    throw new NotImplementedException();
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
          close(rs);
          close(ps);
          close(c);
        }
      } catch (SQLException e) {
        throw new AvroBaseException("Database problem", e);
      }
    }
  }

  private synchronized Schema getSchema(final int schema_id) throws AvroBaseException {
    Schema schema = abbrevSchema.get(schema_id);
    if (schema == null) {
      schema = new Query<Schema>("SELECT id, json FROM " + schemaTable + " WHERE id=?") {
        void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
          ps.setInt(1, schema_id);
        }

        Schema execute(ResultSet rs) throws AvroBaseException, SQLException {
          if (rs.next()) {
            return loadSchema(schema_id, rs.getBytes(2));
          } else {
            return null;
          }
        }
      }.query();
    }
    return schema;
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
        schemaAbbrev.put(schema, id);
      }
    }
    return id;
  }

  private Schema loadSchema(int id, byte[] value) throws AvroBaseException {
    Schema schema;
    try {
      schema = Schema.parse(new ByteArrayInputStream(value));
    } catch (IOException e) {
      throw new AvroBaseException("Could not parse the schema", e);
    }
    abbrevSchema.put(id, schema);
    schemaAbbrev.put(schema, id);
    return schema;
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
          close(rs2);
          close(ps2);
          close(ps);
          close(c);
        }
      } catch (SQLException e) {
        throw new AvroBaseException("Database problem", e);
      }
    }
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
          close(ps);
          close(c);
        }
      } catch (SQLException e) {
        throw new AvroBaseException("Database problem", e);
      }
    }
  }

  private static String columnClause(Iterable<String> strings) {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    for (String str : strings) {
      sb.append(str);
      sb.append(',');
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append(')');
    return sb.toString();
  }

  public static String paramClause(int count) {
    Preconditions.checkArgument(count > 0);

    final StringBuilder sb = new StringBuilder();
    sb.append("(");
    for (int i=0; i<count-1; i++) {
      sb.append("?,");
    }
    sb.append("?)");
    return sb.toString();
  }

  public static void close(Statement obj) {
    if (obj != null) {
      try {
        obj.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  public static void close(Connection obj) {
    if (obj != null) {
      try {
        obj.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  public static void close(ResultSet obj) {
    if (obj != null) {
      try {
        obj.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }
}