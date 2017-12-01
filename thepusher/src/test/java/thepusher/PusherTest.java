package thepusher;

import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static thepusher.PusherTest.SimpleBinding.PASSWORD;
import static thepusher.PusherTest.SimpleBinding.PUSHED;
import static thepusher.PusherTest.SimpleBinding.USERNAME;

/**
 * TODO: Edit this
 * <p/>
 * User: sam
 * Date: Sep 27, 2010
 * Time: 1:19:29 PM
 */
public class PusherTest {

  @Target({ElementType.FIELD, ElementType.PARAMETER})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Push {
    SimpleBinding value();
  }

  public enum SimpleBinding {
    USERNAME,
    PASSWORD,
    PUSHED
  }

  public static class Pushed {
    @Push(USERNAME) String username;
    @Push(PASSWORD) String password;
  }
  
  public static class Pushed2 {
    public Pushed2(@Push(USERNAME) String username, @Push(PASSWORD) String password) {
      this.username = username;
      this.password = password;
    }
    String username;
    String password;
  }

  public static class Pushed3 {
    public Pushed3(@Push(USERNAME) String username) {
      this.username = username;
    }
    String username;
    @Push(PASSWORD) String password;
  }

  @Test
  public void simpleBind() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    p.bindInstance(USERNAME, "sam");
    p.bindInstance(PASSWORD, "blah");
    p.bindInstance(PASSWORD, "blah");

    String username = p.get(USERNAME, String.class);
    assertEquals("sam", username);

    p.bindClass(PUSHED, Pushed.class);

    Pushed pushed = p.get(PUSHED, Pushed.class);
    assertEquals("sam", pushed.username);
    assertEquals("blah", pushed.password);

    p.bindClass(PUSHED, Pushed2.class);

    Pushed2 pushed2 = p.get(PUSHED, Pushed2.class);
    assertEquals("sam", pushed2.username);
    assertEquals("blah", pushed2.password);

    Pushed3 pushed3 = p.create(Pushed3.class);
    assertEquals("sam", pushed3.username);
    assertEquals("blah", pushed3.password);
  }

  public static class B {
  }

  public static class A {
    private final B b;

    public A(@Push(USERNAME) B b) {
      this.b = b;
    }
  }

  public static class Pushed4 {
    @Push(PUSHED) A a;
  }

  @Test
  public void abTest() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    p.bindClass(PUSHED, A.class);
    B b = new B();
    p.bindInstance(USERNAME, b);

    Pushed4 pushed4 = new Pushed4();
    p.push(pushed4);
    assertEquals(b, pushed4.a.b);
  }
  
  public static class D {
    @Push(PUSHED) E e;
  }

  public static class E {
    @Push(PASSWORD) C c;
  }

  public static class C {
    private final D d;

    public C(String s) {
      d = null;
    }

    public C(@Push(USERNAME) D d) {
      this.d = d;
    }
  }

  @Test
  public void cyclicTest() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    p.bindClass(USERNAME, D.class);
    p.bindClass(PASSWORD, C.class);
    p.bindClass(PUSHED, E.class);

    D d = p.get(USERNAME, D.class);
    C c = p.get(PASSWORD, C.class);
    E e = p.get(PUSHED, E.class);
    assertEquals(d, c.d);
    assertEquals(e, d.e);
    assertEquals(c, e.c);
  }

  public static class G {
    G(@Push(USERNAME) H h) {}
  }

  public static class H {
    H(@Push(PASSWORD) G g) {}
  }

  public static class I {
    I(@Push(PASSWORD) G g) {}
  }

  @Test
  public void cyclicConstructor() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    p.bindClass(USERNAME, H.class);
    p.bindClass(PASSWORD, G.class);

    try {
      p.get(USERNAME, H.class);
      fail("Should fail");
    } catch (PusherException pe) {
    }

    try {
      p.create(I.class);
      fail("Should fail");
    } catch (PusherException pe) {
    }

  }

  @Test
  public void notbound() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    try {
      p.create(A.class);
      fail("Should fail");
    } catch (PusherException pe) {
    }
    try {
      p.create(Pushed4.class);
      fail("Should fail");
    } catch (PusherException pe) {
    }
  }


  @Retention(RetentionPolicy.RUNTIME)
  public @interface PushNoTarget {
    SimpleBinding value();
  }

  @Target({ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface PushWrongTarget {
    SimpleBinding value();
  }

  @Target({ElementType.FIELD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface PushOneTarget {
    SimpleBinding value();
  }

  @Target({ElementType.FIELD})
  @Retention(RetentionPolicy.CLASS)
  public @interface PushWrongRetention {
    SimpleBinding value();
  }

  @Target({ElementType.FIELD})
  public @interface PushNoRetention {
    SimpleBinding value();
  }

  @Target({ElementType.FIELD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface PushWrongReturn {
    String value();
  }

  @Target({ElementType.FIELD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface PushNoValue {
  }

  @Test
  public void testAnnotation() {
    try { PusherBase.create(SimpleBinding.class, PushNoTarget.class); fail("should fail"); } catch (PusherException e) {}
    try { PusherBase.create(SimpleBinding.class, PushWrongTarget.class); fail("should fail"); } catch (PusherException e) {}
    PusherBase.create(SimpleBinding.class, PushOneTarget.class);
    try { PusherBase.create(SimpleBinding.class, PushWrongRetention.class); fail("should fail"); } catch (PusherException e) {}
    try { PusherBase.create(SimpleBinding.class, PushNoRetention.class); fail("should fail"); } catch (PusherException e) {}
    try { PusherBase.create(SimpleBinding.class, PushWrongReturn.class); fail("should fail"); } catch (PusherException e) {}
    try { PusherBase.create(SimpleBinding.class, PushNoValue.class); fail("should fail"); } catch (PusherException e) {}
  }

  public static class J {
    J(@Push(PASSWORD) G g, @Deprecated String i) {}
  }

  public static class K {
    K(@Push(PASSWORD) G g) {}
    K(@Push(USERNAME) H h) {}
  }

  @Test
  public void testConstructors() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    p.bindInstance(PASSWORD, null);
    p.bindInstance(PASSWORD, null);
    p.bindInstance(USERNAME, null);
    try {
      p.create(J.class);
      fail("should fail");
    } catch (PusherException e) {}
    try {
      p.create(K.class);
      fail("should fail");
    } catch (PusherException e) {}
  }

  @Test
  public void testWrongType() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    p.bindInstance(USERNAME, "sam");
    p.bindInstance(PASSWORD, 1);

    try {
      p.create(Pushed.class);
      fail("should fail");
    } catch(PusherException e) {}
  }

  public static class L {
  }

  public static class M {
    M(@Push(USERNAME) L l) {}
  }

  @Test
  public void testDepends() {
    {
      Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
      p.bindClass(USERNAME, L.class);

      p.create(M.class);
    }
    {
      Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
      p.bindClass(USERNAME, L.class);

      p.create(M.class);
    }
  }

  public static class N {
    protected @Push(USERNAME) String username;
  }

  public static class O extends N {
    @Push(PASSWORD) String password;
  }

  @Test
  public void testSuperclass() {
    Pusher<SimpleBinding> p = PusherBase.create(SimpleBinding.class, Push.class);
    p.bindInstance(USERNAME, "sam");
    p.bindInstance(PASSWORD, "blah");
    O o = p.create(O.class);
    assertEquals("sam", o.username);
    assertEquals("blah", o.password);
  }
}
