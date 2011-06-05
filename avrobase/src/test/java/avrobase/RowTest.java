package avrobase;

import avrobase.data.Update;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static junit.framework.Assert.assertEquals;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: 5/13/11
 * Time: 9:15 AM
 */
public class RowTest {
  @Test
  public void testRowClone() {
    Update u = new Update();
    u.bag = "23";
    u.owner = "1";
    u.created = System.currentTimeMillis();
    u.updated = System.currentTimeMillis();
    u.image = "1";
    u.type = "update";
    u.text = "This is a test";
    Row<Update, byte[]> row = new Row<Update, byte[]>(u, "12".getBytes());
    Row<Update, byte[]> newrow = row.clone();
    assertEquals(row, newrow);
    System.out.println(newrow);
  }

  @Test
  public void testRowSerialization() throws IOException, ClassNotFoundException {
    Update u = new Update();
    u.bag = "23";
    u.owner = "1";
    u.created = System.currentTimeMillis();
    u.updated = System.currentTimeMillis();
    u.image = "1";
    u.type = "update";
    u.text = "This is a test";
    Row<Update, byte[]> row = new Row<Update, byte[]>(u, "12".getBytes());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(row);
    oos.close();
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
    Row<Update, byte[]> newrow = (Row<Update, byte[]>) ois.readObject();
    assertEquals(row, newrow);
    System.out.println(newrow);
  }
}
