package thepusher;

/**
 * All exceptions in Pusher are runtime exceptions
 * <p/>
 * User: sam
 * Date: Sep 27, 2010
 * Time: 1:52:02 PM
 */
public class PusherException extends RuntimeException {
  public PusherException(Throwable e) {
    super(e);
  }

  public PusherException(String s, NoSuchMethodException e) {
    super(s, e);
  }

  public PusherException(String e) {
    super(e);
  }
}
