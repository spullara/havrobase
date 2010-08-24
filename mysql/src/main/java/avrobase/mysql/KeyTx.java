package avrobase.mysql;

/**
 * Created by IntelliJ IDEA.
 * User: john
 * Date: Aug 21, 2010
 * Time: 12:37:13 AM
 * To change this template use File | Settings | File Templates.
 */
public interface KeyTx<K> {
  byte[] toBytes(K key);

  K fromBytes(byte[] row);
}
