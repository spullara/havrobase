package havrobase;

import java.io.IOException;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Jun 8, 2010
 * Time: 5:27:43 PM
 */
public class HAvroBaseException extends Throwable {
  public HAvroBaseException(Throwable cause) {
    super(cause);
  }

  public HAvroBaseException(String message) {
    super(message);
  }

  public HAvroBaseException(String s, IOException e) {
    super(s, e);
  }
}
