package com.oceanbase.jdbc.internal.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClassLoaderUtils {

    private static final Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, Constructor<?>> NO_ARG_CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();

    /**
     * Create an instance using the no-argument constructor
     */
    public static <T> T newInstance(Class<T> returnType, String className) throws ReflectiveOperationException {
        return newInstance(returnType, className, (ClassLoader) null);
    }

    /**
     * Create an instance using the specified class loader and the no-argument constructor.
     */
    public static <T> T newInstance(Class<T> returnType, String className, ClassLoader classLoader) throws ReflectiveOperationException {
        Class<?> clazz = getClass(className, classLoader);
        if (!returnType.isAssignableFrom(clazz)) {
            throw new ClassCastException("Class " + className + " is not assignable to " + returnType.getName());
        }
        Constructor<?> constructor = getNoArgConstructor(clazz);
        try {
            return (T) constructor.newInstance();
        } catch (IllegalAccessException e) {
            throw new InstantiationException("Cannot access constructor of " + className);
        } catch (InvocationTargetException e) {
            throw new InstantiationException("Constructor threw an exception for " + className);
        }
    }

    /**
     * Create an instance using the specified parameter types and argument values.
     */
    public static <T> T newInstance(Class<T> returnType, String className, Class<?>[] parameterTypes, Object... args)
            throws ReflectiveOperationException {
        return newInstance(returnType, className, null, parameterTypes, args);
    }

    /**
     * Create an instance using the specified class loader, parameter types, and parameter values.
     */
    public static <T> T newInstance(Class<T> returnType, String className, ClassLoader classLoader, Class<?>[] parameterTypes, Object... args)
            throws ReflectiveOperationException {
        if (parameterTypes == null) {
            parameterTypes = new Class<?>[0];
        }
        if (args == null) {
            args = new Object[0];
        }
        if (parameterTypes.length != args.length) {
            throw new IllegalArgumentException("Parameter types and arguments length mismatch");
        }

        Class<?> clazz = getClass(className, classLoader);
        if (!returnType.isAssignableFrom(clazz)) {
            throw new ClassCastException("Class " + className + " is not assignable to " + returnType.getName());
        }

        Constructor<?> constructor = clazz.getDeclaredConstructor(parameterTypes);
        try {
            constructor.setAccessible(true);
        } catch (SecurityException ignored) {
            // ignored
        }
        try {
            return (T) constructor.newInstance(args);
        } catch (IllegalAccessException e) {
            throw new InstantiationException("Cannot access constructor of " + className);
        } catch (InvocationTargetException e) {
            throw new InstantiationException("Constructor threw an exception for " + className);
        }
    }

    /**
     * Obtain the class object.
     */
    private static Class<?> getClass(String className, ClassLoader classLoader) throws ClassNotFoundException {
        if (classLoader == null) {
            classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = ClassLoaderUtils.class.getClassLoader();
            }
        }

        Class<?> clazz = CLASS_CACHE.get(className);
        if (clazz == null) {
            synchronized (ClassLoaderUtils.class) {
                clazz = CLASS_CACHE.get(className);
                if (clazz == null) {
                    clazz = Class.forName(className, true, classLoader);
                    CLASS_CACHE.put(className, clazz);
                }
            }
        }
        return clazz;
    }

    /**
     * Obtain the no-argument constructor.
     */
    private static Constructor<?> getNoArgConstructor(Class<?> clazz) throws NoSuchMethodException {
        String className = clazz.getName();
        Constructor<?> constructor = NO_ARG_CONSTRUCTOR_CACHE.get(className);
        if (constructor == null) {
            synchronized (ClassLoaderUtils.class) {
                constructor = NO_ARG_CONSTRUCTOR_CACHE.get(className);
                if (constructor == null) {
                    constructor = clazz.getDeclaredConstructor();
                    try {
                        constructor.setAccessible(true);
                    } catch (SecurityException ignored) {
                        //ignored
                    }
                    NO_ARG_CONSTRUCTOR_CACHE.put(className, constructor);
                }
            }
        }
        return constructor;
    }
}
