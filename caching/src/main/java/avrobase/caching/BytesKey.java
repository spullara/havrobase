package avrobase.caching;

import java.io.Serializable;

/**
* TODO: Edit this
* <p/>
* User: sam
* Date: 5/11/11
* Time: 7:15 PM
*/
public class BytesKey implements Serializable {
  private final byte[] key;

  public BytesKey(byte[] key) {
    this.key = key;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof BytesKey) {
      BytesKey other = (BytesKey) o;
      byte[] otherkey = other.key;
      if (key.length == otherkey.length) {
        int i = 0;
        for (byte b : key) {
          if (b != otherkey[i++]) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashcode = 0;
    for (byte aKey : key) {
      hashcode += aKey + hashcode * 43;
    }
    return hashcode;
  }
}
