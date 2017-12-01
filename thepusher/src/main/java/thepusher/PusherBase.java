package thepusher;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * Pushes values into objects. Supports fields and constructors.
 * <p/>
 *
 * @author Sam Pullara
 * @author John Beatty
 *         Date: Sep 27, 2010
 *         Time: 1:14:48 PM
 */
@SuppressWarnings({"unchecked"})
public class PusherBase<E> implements Pusher<E> {
  private static Object NULL = new Object();
  private final static Logger logger = Logger.getLogger(Pusher.class.getName());
  private final Class<? extends Annotation> pushAnnotation;
  private final Method valueMethod;

  private PusherBase(Class<E> bindingEnumeration, Class<? extends Annotation> pushAnnotation) {
    this.pushAnnotation = pushAnnotation;
    try {
      valueMethod = pushAnnotation.getMethod("value");
      if (valueMethod.getReturnType() != bindingEnumeration) {
        throw new PusherException("Return type of value() method must be the enumeration type");
      }
      Target target = pushAnnotation.getAnnotation(Target.class);
      if (target == null) {
        throw new PusherException("No Target annotation on annotation, must target parameters and/or fields");
      }
      boolean parameterOrField = false;
      LOOP:
      for (ElementType etype : target.value()) {
        switch (etype) {
          case FIELD:
          case PARAMETER:
            parameterOrField = true;
            break LOOP;
        }
      }
      if (!parameterOrField) {
        throw new PusherException("Annotation must target parameters and/or fields");
      }
      Retention retention = pushAnnotation.getAnnotation(Retention.class);
      if (retention == null) {
        throw new PusherException("No retention policy set on annotation, you must set it to RUNTIME");
      } else if (retention.value() != RetentionPolicy.RUNTIME) {
        throw new PusherException("Annotation retention policy must be set to RUNTIME");
      }
    } catch (NoSuchMethodException e) {
      throw new PusherException("Annotation missing value() method", e);
    }
  }

  private Map<E, Class> classBindings = new ConcurrentHashMap<E, Class>();
  private Map<E, Object> instanceBindings = new ConcurrentHashMap<E, Object>();

  @Override
  public <T> void bindClass(E binding, Class<T> type) {
    classBindings.put(binding, type);
  }

  @Override
  public <T> T create(Class<T> type) {
    return push(instantiate(type));
  }

  private static final Map<Class, Constructor[]> constructorMap = new ConcurrentHashMap<Class, Constructor[]>();

  private <T> T instantiate(Class<T> type) {
    try {
      T o = null;
      Constructor<?>[] declaredConstructors = constructorMap.get(type);
      boolean cacheit = false;
      if (declaredConstructors == null) {
        cacheit = true;
        declaredConstructors = type.getDeclaredConstructors();
      }
      Object[] parameterValues = null;
      for (Constructor constructor : declaredConstructors) {
        constructor.setAccessible(true);
        Annotation[][] parameterAnnotations = constructor.getParameterAnnotations();
        int length = parameterAnnotations.length;
        if (length == 0) continue;
        for (int i = 0; i < length; i++) {
          Annotation foundAnnotation = null;
          for (Annotation parameterAnnotation : parameterAnnotations[i]) {
            if (parameterAnnotation.annotationType().equals(pushAnnotation)) {
              foundAnnotation = parameterAnnotation;
              break;
            }
          }
          if (foundAnnotation == null) {
            if (i == 0) {
              // This constructor is not a candidate
              break;
            }
            throw new PusherException("All parameters of constructor must be annotated: " + constructor);
          } else if (i == 0) {
            if (parameterValues != null) {
              throw new PusherException("Already found a valid constructor");
            }
            parameterValues = new Object[length];
          }
          parameterValues[i] = getOrCreate((E) valueMethod.invoke(foundAnnotation));
        }
        if (parameterValues != null) {
          if (cacheit) constructorMap.put(type, new Constructor[]{constructor});
          o = (T) constructor.newInstance(parameterValues);
        }
      }
      if (o == null) {
        if (cacheit) constructorMap.put(type, new Constructor[0]);
        o = type.newInstance();
      }
      return o;
    } catch (Exception e) {
      throw new PusherException(e);
    }
  }

  private Object get(E key) {
    Object value = instanceBindings.get(key);
    if (value == NULL) return null;
    return value;
  }

  private static final Map<Class, Map<Class, List<Field>>> fieldsPushMap = new ConcurrentHashMap<Class, Map<Class, List<Field>>>();

  @Override
  @SuppressWarnings({"unchecked"})
  public <T> T push(T o) {
    try {
      List<Field> fields;
      Class aClass = o.getClass();
      do {
        Map<Class, List<Field>> classListMap = fieldsPushMap.get(aClass);
        if (classListMap == null) {
          classListMap = new ConcurrentHashMap<Class, List<Field>>();
          fieldsPushMap.put(aClass, classListMap);
        }
        fields = classListMap.get(pushAnnotation);
        if (fields == null) {
          fields = ImmutableList.copyOf(Iterables.filter(Arrays.asList(aClass.getDeclaredFields()), new Predicate<Field>() {
            public boolean apply(Field field) {
              Annotation annotation = field.getAnnotation(pushAnnotation);
              if (annotation != null) {
                field.setAccessible(true);
              }
              return annotation != null;
            }
          }));
          classListMap.put(pushAnnotation, fields);
        }
        for (Field field : fields) {
          E fieldBinding = (E) valueMethod.invoke(field.getAnnotation(pushAnnotation));
          Object bound = getOrCreate(fieldBinding);
          field.set(o, bound);
        }
      } while ((aClass = aClass.getSuperclass()) != Object.class);
      return o;
    } catch (PusherException e) {
      throw e;
    } catch (Exception e) {
      throw new PusherException(e);
    }
  }

  private <T> Object getOrCreate(E fieldBinding) {
    Object bound;
    Class removed = classBindings.remove(fieldBinding);
    if (removed != null) {
      rebind(fieldBinding, instantiate(removed));
    }
    if (instanceBindings.containsKey(fieldBinding)) {
      bound = get(fieldBinding);
      if (removed != null) {
        push(bound);
      }
    } else {
      throw new PusherException(fieldBinding + " is not bound");
    }
    return bound;
  }

  @Override
  public <T> void bindInstance(E binding, T instance) {
    rebind(binding, instance);
  }

  private void rebind(E binding, Object instance) {
    if (instance == null) {
      instance = NULL;
    }
    Object alreadyBound = instanceBindings.put(binding, instance);
    if (alreadyBound == NULL) {
      alreadyBound = null;
    }
    if (alreadyBound != null) {
      logger.warning("Binding rebound: " + binding + " was " + alreadyBound);
    }
  }

  @Override
  public <F> F get(E binding, Class<F> type) {
    Class removed = classBindings.remove(binding);
    if (removed != null) {
      Object o = instantiate(removed);
      rebind(binding, o);
      push(o);
    }
    return (F) get(binding);
  }

  /**
   * Create a new base Pusher.
   *
   * @param bindingEnumeration
   * @param <E>
   * @return
   */
  public static <E> Pusher<E> create(Class<E> bindingEnumeration, Class<? extends Annotation> pushAnnotation) {
    return new PusherBase<E>(bindingEnumeration, pushAnnotation);
  }
}
