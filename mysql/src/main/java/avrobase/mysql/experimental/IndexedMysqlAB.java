package avrobase.mysql.experimental;

import avrobase.*;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
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
 * An experimental MySQL implementation of AvroBase that has a few interesting features:
 *  - supports same-table indexes, giving users simple transactional indexes. the indexcolumn can specify uniqueness contraints.
 *  - supports many-to-many relation tables, which is a combination of two indexes with a unique index capturing both columns
 *  - supports audit values within avro objects -- "updated" and "created" -- where the current timestamp will be inserted into the avro object upon create/put, only if the object has those fields.
 *
 * Limitations:
 *  - only supports bigint keys right now
 *
 * Implementation Notes:
 *  - quite a bit of copy/paste from MysqlAB. This is just an experiment for now.
 *
 * @param <T>
 */
public class IndexedMysqlAB<T extends SpecificRecord> extends AvroBaseImpl<T, Long> implements Searchable<T,Long, IndexQuery<?>> {
  private static final Joiner COMMAS = Joiner.on(',').skipNulls();
  private static final Joiner SPACES = Joiner.on(' ').skipNulls();
  private static final Joiner EQUALS = Joiner.on('=');
  private static final AvroFormat AVRO_FORMAT = AvroFormat.BINARY;
  private static final String[] BASE_COLS = new String[]{"id", "version", "schema_id", "avro"};

  private static final String FIELD_UPDATED = "updated";
  private static final String FIELD_CREATED = "created";

  private final DataSource datasource;
  private final String schemaTable;
  private final String table;
  private final Supplier<Long> keySupplier;
  private final Iterable<IndexColumn<T, ?>> indexColumns;
  private final @Nullable ManyManyRelation<T> relation;
  private final boolean createTable;

  // schema cache
  private final Map<Integer, Schema> schemas = new ConcurrentHashMap<Integer, Schema>();
  private final Map<Schema, Integer> schemaIds = new ConcurrentHashMap<Schema, Integer>();

  // pre-computed SQL statements
  private final String insertStatement;
  private final String updateStatement;
  private final String upsertStatement;
  private final String selectStatement;
  private final String selectIndexBaseStatement;

  public IndexedMysqlAB(DataSource datasource, String table, Schema schema, String schemaTable, Supplier<Long> keySupplier, Iterable<IndexColumn<T, ?>> indexColumns, @Nullable ManyManyRelation<T> relation, boolean createTable) {
    super(schema, AVRO_FORMAT);
    this.datasource = checkNotNull(datasource);
    this.table = checkNotNull(table);
    this.schemaTable = checkNotNull(schemaTable);
    this.indexColumns = checkNotNull(indexColumns);
    this.relation = relation;
    this.keySupplier = checkNotNull(keySupplier);
    this.createTable = createTable;

    final String insertColsClause = COMMAS.join(orderedColNames(relation, indexColumns));
    final String insertParamClause = paramClause(BASE_COLS.length + Iterables.size(indexColumns) + (relation != null ? 2 : 0));
    insertStatement = "INSERT INTO " + table + " (" + insertColsClause + ") VALUES " + insertParamClause;
    upsertStatement = "INSERT INTO " + table + insertColsClause + " VALUES " + insertParamClause + " ON DUPLICATE KEY UPDATE schema_id=values(schema_id), version = version + 1, avro=values(avro)";
    updateStatement = "UPDATE " + table + " SET schema_id=?,version=version+1,avro=?" + updateIndexClause() + " WHERE id=? AND version=?";
    selectStatement = "SELECT schema_id, version, avro FROM " + table + " WHERE id=?";
    selectIndexBaseStatement = "SELECT id, schema_id, version, avro FROM " + table + " WHERE";

    createTables();
  }

  // TODO: ugly
  public String updateIndexClause() {
    List<String> clauses = new LinkedList<String>();

    if (relation != null) {
      clauses.add(relation.getLeft().getColumnName() + "=?");
      clauses.add(relation.getRight().getColumnName() + "=?");
    }
    for (IndexColumn<?, ?> idx : indexColumns) {
      clauses.add(idx.getColumnName() + "=?");
    }

    if (clauses.size() > 0) {
      return "," + COMMAS.join(clauses);
    } else {
      return "";
    }
  }

  public String generateDdl() {
    final List<String> clauses = new LinkedList<String>();

    // base cols
    clauses.add("id bigint not null primary key");
    clauses.add("version integer not null");

    if (relation != null) {
      clauses.add(SPACES.join(relation.getLeft().getColumnName(), "bigint not null"));
      clauses.add(SPACES.join(relation.getRight().getColumnName(), "bigint not null"));
    }

    for (IndexColumn<T, ?> ic : indexColumns) {
      clauses.add(SPACES.join(ic.getColumnName(), ' ', ic.getColumnTypeString(), (ic.isNullable() ? "not null" : null)));
    }

    // data cols
    clauses.add("schema_id integer not null");
    clauses.add("avro mediumblob not null");

    if (relation != null) {
      clauses.add(SPACES.join("UNIQUE INDEX (", COMMAS.join(relation.getLeft().getColumnName(), relation.getRight().getColumnName()), ')'));
      clauses.add(SPACES.join("INDEX (", relation.getRight().getColumnName(), ')'));
    }

    for (IndexColumn<T, ?> ic : indexColumns) {
      clauses.add(SPACES.join((ic.isUnique() ? "UNIQUE" : null), "INDEX (", ic.getColumnName(), ')'));
    }

    return SPACES.join("CREATE TABLE", table, '(', COMMAS.join(clauses), ')', "ENGINE=InnoDB DEFAULT CHARSET=UTF8");
  }


  public void createTables() {
    try {
      Connection connection = null;
      try {
        connection = datasource.getConnection();

        DatabaseMetaData data = connection.getMetaData();
        {
          ResultSet tables = data.getTables(null, null, table, null);
          if (!tables.next()) {
            if (createTable) {
              Statement statement = connection.createStatement();
              String createString = generateDdl();
              statement.executeUpdate(createString);
              close(statement);
            } else {
              System.out.println("You need this DDL:\n"+ generateDdl());
              throw new AvroBaseException("table does not exist: " + table);
            }
          }
          tables.close();
        }
        {
          ResultSet tables = data.getTables(null, null, this.schemaTable, null);
          if (!tables.next()) {
            final String ddl = "CREATE TABLE " + this.schemaTable + " (id integer primary key auto_increment, json longblob not null)";
            if (createTable) {
              Statement statement = connection.createStatement();
              statement.executeUpdate(ddl);
              statement.close();
            } else {
              System.out.println("You need this DDL:\n"+ ddl);
              throw new AvroBaseException("schema table does not exist: " + schemaTable);
            }
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
        close(connection);
      }
    } catch (SQLException sqle) {
      throw new AvroBaseException("Problem with MySQL", sqle);
    }
  }

  public Iterable<Row<T,Long>> select(IndexColumn column, final Object value) {
    return indexSelect(column.getColumnName(), column.getColumnSqlType(), value);
  }

  public Row<T,Long> lookup(IndexQuery<?> selector) {
    return Iterables.getOnlyElement(indexSelect(selector.getColumn().getColumnName(), selector.getColumn().getColumnSqlType(), selector.getValue()), null);
  }

  protected Iterable<Row<T,Long>> indexSelect(String column, final int sqlType, final Object value) {
    final String query = selectIndexBaseStatement + " " + column + "=?";

    return new Query<Iterable<Row<T, Long>>>(query) {
      void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        ps.setObject(1, value, sqlType);
      }

      Iterable<Row<T, Long>> execute(ResultSet rs) throws AvroBaseException, SQLException {
        final List<Row<T,Long>> results = new LinkedList<Row<T,Long>>();

        while (rs.next()) {
          long row = rs.getLong(1);
          int schema_id = rs.getInt(2);
          long version = rs.getLong(3);
          byte[] avro = rs.getBytes(4);
          Schema schema = getSchema(schema_id);
          if (schema != null) {
            results.add(new Row<T, Long>(readValue(avro, schema, format), row, version));
          } else {
            throw new AvroBaseException("Failed to find schema: " + schema_id);
          }
        }
        return results;
      }
    }.query();
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
    setTimestamp(value, FIELD_CREATED);
    final long key = keySupplier.get();
    if (!put(key, value, 0)) {
      throw new AvroBaseException("did not add " + key);
    } else {
      return key;
    }
  }

  @Override
  public void put(final Long id, final T value) throws AvroBaseException {
    setTimestamp(value, FIELD_UPDATED);
    int updated = new Update(upsertStatement) {
      void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        parameterize(ps, id, 1, schemaId(value), value);
      }
    }.insert();
    if (updated == 0) {
      throw new AvroBaseException("Failed to save: " + updated);
    }
  }

  /**
   * returns the schema ID to use for the given value, creating it if necessary
   * @param value
   * @return
   */
  private Integer schemaId(T value) {
    Schema schema = value.getSchema();
    Integer schemaId = schemaIds.get(schema);
    if (schemaId == null) {
      schemaId = storeSchema(schema);
    }
    return schemaId;
  }

  /**
   * return list of column names in the sanctioned order.
   */
  private List<String> orderedColNames(ManyManyRelation relation, Iterable<IndexColumn<T, ?>> idxColumns) {
    // Order: base cols, relation cols, other index cols
    List<String> cols = new ArrayList<String>();
    for (String col : BASE_COLS) {
      cols.add(col);
    }
    if (relation != null) {
      cols.add(relation.getLeft().getColumnName());
      cols.add(relation.getRight().getColumnName());
    }
    for (IndexColumn<?, ?> idx : idxColumns) {
      cols.add(idx.getColumnName());
    }
    return cols;
  }

  /**
   * updates the timestamp if the schema has an 'updated' field
   * @param value
   */
  private void setTimestamp(T value, String field) {
    Schema.Field f = value.getSchema().getField(field);
    if (f != null) {
      value.put(f.pos(), System.currentTimeMillis());
    }
  }


  @Override
  public boolean put(final Long id, final T value, final long version) throws AvroBaseException {
    setTimestamp(value, FIELD_UPDATED);
    final Integer schemaId = schemaId(value);
    if (version == 0) {
      try {
        int updated = new Update(insertStatement) {
          void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
            parameterize(ps, id, 1, schemaId, value);
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
      int updated = new Update(updateStatement) {
        void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
          int i = 1;
          ps.setInt(i++, schemaId);
          ps.setBytes(i++, serialize(value));

          if (relation != null) {
            ps.setLong(i++, relation.getLeft().valueFunction().apply(value));
            ps.setLong(i++, relation.getRight().valueFunction().apply(value));
          }

          for (IndexColumn<T, ?> ic : indexColumns) {
            ps.setObject(i++, ic.valueFunction().apply(value), ic.getColumnSqlType());
          }

          ps.setLong(i++, id);
          ps.setLong(i, version);
        }
      }.insert();
      if (updated == 0) {
        return false;
      }
    }
    return true;
  }

  private void parameterize(PreparedStatement ps, Long id, int version, Integer schemaId, T value) throws SQLException {
    int i = 1;
    ps.setLong(i++, id);
    ps.setInt(i++, version);
    ps.setInt(i++, schemaId);
    ps.setBytes(i++, serialize(value));

    if (relation != null) {
      ps.setLong(i++, relation.getLeft().valueFunction().apply(value));
      ps.setLong(i++, relation.getRight().valueFunction().apply(value));
    }

    for (IndexColumn<T, ?> ic : indexColumns) {
      ps.setObject(i++, ic.valueFunction().apply(value), ic.getColumnSqlType());
    }
  }

  @Override
  public void delete(Long row) throws AvroBaseException {
    //TODO:1
    throw new NotImplementedException();
  }

  @Override
  public Iterable<Row<T, Long>> scan(Long startRow, Long stopRow) throws AvroBaseException {
    //throw new NotImplementedException();
    //TODO:0
    return Collections.emptyList();
  }

  @Override
  public Iterable<Row<T, Long>> search(IndexQuery query) throws AvroBaseException {
    return select(query.getColumn(), query.getColumn());
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
    Schema schema = schemas.get(schema_id);
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
      id = schemaIds.get(schema);
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
        schemas.put(id, schema);
        schemaIds.put(schema, id);
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
    schemas.put(id, schema);
    schemaIds.put(schema, id);
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

  public static String paramClause(int count) {
    Preconditions.checkArgument(count > 0);

    final StringBuilder sb = new StringBuilder();
    sb.append("(");
    sb.append(Strings.repeat("?,", count));
    sb.deleteCharAt(sb.length() - 1);
    sb.append(")");
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