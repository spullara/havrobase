package avrobase;

import java.io.UnsupportedEncodingException;

/**
 * Byte utilities.
 *
 * @author john
 */
public class Bytes {
  public static byte[] utf8Bytes(String string) {
    try {
      return string.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AvroBaseException(e);
    }
  }


  public static String utf8String(byte[] bytes) {
    if (bytes.length == 0) {
      return "";
    } else {
      try {
        return new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new AvroBaseException(e);
      }
    }
  }
}