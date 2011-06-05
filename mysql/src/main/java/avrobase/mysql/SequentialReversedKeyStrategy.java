package avrobase.mysql;

import avrobase.AvroBaseException;
import avrobase.mysql.KeyStrategy;
import avrobase.mysql.MysqlAB;
import com.google.common.base.Charsets;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Uses auto_increment in MySQL to maintain the sequential values and then
 * textually reverses them for distribution.
 * <p/>
 * User: sam
 * Date: 11/1/10
 * Time: 1:37 PM
 */
public class SequentialReversedKeyStrategy implements KeyStrategy<byte[]> {
  private final DataSource ds;
  private String tableName;

  public SequentialReversedKeyStrategy(DataSource ds, String table, String family) {
    this.ds = ds;
    tableName = table + "__" + family + "_" + "ids";
    Connection connection = null;
    try {
      connection = ds.getConnection();
      DatabaseMetaData data = connection.getMetaData();
      {
        ResultSet tables = data.getTables(null, null, tableName, null);
        if (!tables.next()) {
          // Create the table
          Statement statement = connection.createStatement();
          statement.executeUpdate("CREATE TABLE " + tableName + " (id bigint auto_increment primary key not null)");
          statement.close();
        }
        tables.close();
      }
    } catch (Exception e) {
      throw new AvroBaseException("Could not create table: " + tableName, e);
    } finally {
      if (connection != null) try {
        connection.close();
      } catch (SQLException e) {
        throw new AvroBaseException("Could not close connection", e);
      }
    }
  }

  @Override
  public byte[] toBytes(byte[] key) {
    return key;
  }

  @Override
  public byte[] fromBytes(byte[] row) {
    return row;
  }

  @Override
  public byte[] fromString(String key) {
    return key.getBytes(Charsets.UTF_8);
  }

  @Override
  public String toString(byte[] row) {
    return new String(row, Charsets.UTF_8);
  }

  @Override
  public byte[] newKey() {
    Connection c = null;
    try {
      c = ds.getConnection();
      PreparedStatement insertRow = c.prepareStatement("INSERT INTO " + tableName + " () VALUES ()");
      int insert = insertRow.executeUpdate();
      if (insert != 1) throw new AvroBaseException("Could not get new key: " + insert + " rows updated");
      insertRow.close();
      PreparedStatement getRow = c.prepareStatement("SELECT LAST_INSERT_ID()");
      ResultSet resultSet = getRow.executeQuery();
      if (resultSet.next()) {
        Long query = resultSet.getLong(1);
        int deleted = c.prepareStatement("DELETE FROM " + tableName + " WHERE id = LAST_INSERT_ID()").executeUpdate();
        if (deleted != 1) throw new AvroBaseException("Failed to delete row");
        byte[] row = String.valueOf(query).getBytes();
        int length = row.length;
        for (int i = 0; i < length / 2; i++) {
          byte tmp = row[i];
          row[i] = row[length - i - 1];
          row[length - i - 1] = tmp;
        }
        return row;
      }
      throw new AvroBaseException("Failed to find last insert id");
    } catch (Exception e) {
      throw new AvroBaseException("Failed to get key", e);
    } finally {
      try {
        c.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  public void setLast(byte[] row) {
    int length = row.length;
    for (int i = 0; i < length / 2; i++) {
      byte tmp = row[i];
      row[i] = row[length - i - 1];
      row[length - i - 1] = tmp;
    }
    final long last = Long.parseLong(new String(row, Charsets.UTF_8)) + 1;
    int insert = new MysqlAB.Update(ds, "ALTER TABLE " + tableName + " AUTO_INCREMENT = ?") {
      public void setup(PreparedStatement ps) throws AvroBaseException, SQLException {
        ps.setLong(1, last);
      }
    }.insert();
  }
}
