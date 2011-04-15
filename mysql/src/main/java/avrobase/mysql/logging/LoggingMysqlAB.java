package avrobase.mysql.logging;

import avrobase.AvroBaseException;
import avrobase.AvroFormat;
import avrobase.mysql.KeyStrategy;
import avrobase.mysql.MysqlAB;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This needs multiserver synchronization if you don't want to federate queries across log tables.
 * <p/>
 * User: sam
 * Date: 4/14/11
 * Time: 11:17 PM
 */
public class LoggingMysqlAB<T extends SpecificRecord, K> extends MysqlAB<T, K> {
  private String logTableName;
  private AtomicInteger count;
  private ReadWriteLock lock = new ReentrantReadWriteLock();

  public LoggingMysqlAB(ExecutorService es, DataSource datasource, String table, String family, String schemaTable, Schema schema, AvroFormat storageFormat, KeyStrategy<K> keytx) throws AvroBaseException {
    super(es, datasource, table, family, schemaTable, schema, storageFormat, keytx);
    try {
      roll();
    } catch (SQLException e) {
      throw new AvroBaseException("Could not roll log table", e);
    }
  }

  public void roll() throws SQLException {
    Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      long id = System.currentTimeMillis() / 1000;
      logTableName = mysqlTableName + "_" + id;
      Connection connection = datasource.getConnection();
      DatabaseMetaData data = connection.getMetaData();
      {
        ResultSet tables = data.getTables(null, null, logTableName, null);
        if (!tables.next()) {
          // Create the table
          Statement statement = connection.createStatement();
          statement.executeUpdate("CREATE TABLE " + logTableName + " ( row varbinary(256) primary key, schema_id integer not null, version integer not null, format tinyint not null, avro mediumblob not null ) ENGINE=INNODB");
          statement.close();
        }
        tables.close();
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  protected void log(final byte[] row, final Integer schemaId, final int format, final byte[] serialized, final long version) {
    es.submit(new Runnable() {
      @Override
      public void run() {
        Lock readLock = lock.readLock();
        readLock.lock();
        try {
          count.getAndAdd(new Insert(datasource, "INSERT INTO " + logTableName + " (row, schema_id, version, format, avro) VALUES (?,?,?,?,?)") {
            public void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
              ps.setBytes(1, row);
              ps.setInt(2, schemaId);
              ps.setLong(3, version);
              ps.setInt(4, format);
              ps.setBytes(5, serialized);
            }
          }.insert());
        } finally {
          readLock.unlock();
        }
      }
    });
  }
}
