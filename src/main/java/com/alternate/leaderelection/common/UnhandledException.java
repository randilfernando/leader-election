package com.alternate.leaderelection.common;

/**
 * @author randilfernando
 */
public final class UnhandledException {

    private UnhandledException() {
    }

    public static <T> T unhandled(UnhandledFunction<T> function) {
        try {
            return function.apply();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static void unhandled(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public interface UnhandledFunction<T> {
        T apply() throws Throwable;
    }
}
