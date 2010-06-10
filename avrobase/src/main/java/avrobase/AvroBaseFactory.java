package avrobase;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.avro.specific.SpecificRecord;

/**
 * Create instances of avrobases.
 * <p/>
 * User: sam
 * Date: Jun 9, 2010
 * Time: 11:51:40 AM
 */
public class AvroBaseFactory {
  public static <T extends SpecificRecord> AvroBase<T> createAvroBase(Module module, Class<? extends AvroBase> clazz, final byte[] table, final byte[] family, final AvroFormat format) throws AvroBaseException {
    Injector injector = Guice.createInjector(module);
    AvroBase base = injector.createChildInjector(new Module() {
      @Override
      public void configure(Binder binder) {        
        binder.bind(byte[].class).annotatedWith(Names.named("table")).toInstance(table);
        binder.bind(byte[].class).annotatedWith(Names.named("family")).toInstance(family);
        binder.bind(AvroFormat.class).toInstance(format);
      }
    }).getInstance(clazz);
    return base;
  }
}
