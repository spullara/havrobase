package avrobase;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.avro.specific.SpecificRecord;

/**
 * Create instances of avrobases.
 * <p/>
 * User: sam
 * Date: Jun 9, 2010
 * Time: 11:51:40 AM
 */
public class AvroBaseFactory {
  public static <T extends SpecificRecord, K, Q> AvroBase<T, K> createAvroBase(Module module, Class<? extends AvroBase> clazz, final AvroFormat format) throws AvroBaseException {
    Injector injector = Guice.createInjector(module);
    return injector.createChildInjector(new Module() {
      @Override
      public void configure(Binder binder)  {        
        binder.bind(AvroFormat.class).toInstance(format);
      }
    }).getInstance(clazz);
  }
}
