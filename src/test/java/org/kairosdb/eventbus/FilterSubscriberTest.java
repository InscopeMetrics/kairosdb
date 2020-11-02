package org.kairosdb.eventbus;


import com.google.common.eventbus.AllowConcurrentEvents;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FilterSubscriberTest {

    private static final String FIXTURE_ARGUMENT = "fixture argument";

    private FilterEventBus bus;
    private boolean methodCalled;
    private Object methodArgument;

    @Before
    public void setUp() throws Exception {
        bus = new FilterEventBus(new EventBusConfiguration(new Properties()));
        methodCalled = false;
        methodArgument = null;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreate_prioityNegative_invalid() {
        FilterSubscriber.create(bus, this, getTestSubscriberMethod("recordingMethod"), -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreate_prioityTooLarge_invalid() {
        FilterSubscriber.create(bus, this, getTestSubscriberMethod("recordingMethod"), 101);
    }

    /*@Test
    public void testCreate() {
        FilterSubscriber s1 = FilterSubscriber.create(bus, this, getTestSubscriberMethod("recordingMethod"), 10);
        assertThat(s1, instanceOf(FilterSubscriber.SynchronizedFilterSubscriber.class));

        // a thread-safe method should not create a synchronized subscriber
        FilterSubscriber s2 = FilterSubscriber.create(bus, this, getTestSubscriberMethod("threadSafeMethod"), 10);
        assertThat(s2, not(instanceOf(FilterSubscriber.SynchronizedFilterSubscriber.class)));
    }*/

    @Test
    public void testInvokeSubscriberMethod_basicMethodCall() throws Throwable {
        final Method method = getTestSubscriberMethod("recordingMethod");
        final FilterSubscriber subscriber = FilterSubscriber.create(bus, this, method, 0);

        final Object result = subscriber.invokeSubscriberMethod(FIXTURE_ARGUMENT);

        assertTrue("Subscriber must call provided method", methodCalled);
        assertTrue("Subscriber argument must be exactly the provided object.",
                methodArgument == FIXTURE_ARGUMENT);
        assertThat(result, instanceOf(String.class));
        assertThat(result, equalTo(FIXTURE_ARGUMENT));
    }

    @Test
    public void testInvokeSubscriberMethod_exceptionWrapping() throws Throwable {
        final Method method = getTestSubscriberMethod("exceptionThrowingMethod");
        final FilterSubscriber subscriber = FilterSubscriber.create(bus, this, method, 100);

        try {
            subscriber.invokeSubscriberMethod(FIXTURE_ARGUMENT);
            fail("Subscribers whose methods throw must throw InvocationTargetException");
        } catch (final InvocationTargetException expected) {
            assertThat(expected.getCause(), instanceOf(IntentionalException.class));
        }
    }

    @SuppressWarnings("EmptyCatchBlock")
    @Test
    public void testInvokeSubscriberMethod_errorPassthrough() throws Throwable {
        final Method method = getTestSubscriberMethod("errorThrowingMethod");
        final FilterSubscriber subscriber = FilterSubscriber.create(bus, this, method, 10);

        try {
            subscriber.dispatchEvent(FIXTURE_ARGUMENT);
            fail("Subscribers whose methods throw Errors must rethrow them");
        } catch (final JudgmentError expected) {
        }
    }

    @Test
    public void test_dispatch() {
        final Method method = getTestSubscriberMethod("recordingMethod");
        final FilterSubscriber subscriber = FilterSubscriber.create(bus, this, method, 10);

        final Object result = subscriber.dispatchEvent(FIXTURE_ARGUMENT);

        assertTrue("Subscriber must call provided method", methodCalled);
        assertTrue("Subscriber argument must be exactly the provided object.",
                methodArgument == FIXTURE_ARGUMENT);
        assertThat(result, instanceOf(String.class));
        assertThat(result, equalTo(FIXTURE_ARGUMENT));
    }

    private Method getTestSubscriberMethod(final String name) {
        try {
            return getClass().getDeclaredMethod(name, Object.class);
        } catch (final NoSuchMethodException e) {
            throw new AssertionError();
        }
    }

    /**
     * Records the provided object in {@link #methodArgument} and sets {@link #methodCalled}.  This
     * method is called reflectively by Subscriber during tests, and must remain public.
     *
     * @param arg argument to record.
     */
    @SuppressWarnings("unused")
    @Subscribe
    public Object recordingMethod(final Object arg) {
        assertFalse(methodCalled);
        methodCalled = true;
        methodArgument = arg;
        return arg;
    }

    @SuppressWarnings("unused")
    @Subscribe
    public void exceptionThrowingMethod(final Object arg) throws Exception {
        throw new IntentionalException();
    }

    @SuppressWarnings("unused")
    @Subscribe
    public void errorThrowingMethod(final Object arg) {
        throw new JudgmentError();
    }

    @SuppressWarnings("unused")
    @Subscribe
    @AllowConcurrentEvents
    public void threadSafeMethod(final Object arg) {
    }

    /**
     * Local exception subclass to check variety of exception thrown.
     */
    private class IntentionalException extends Exception {

        private static final long serialVersionUID = -2500191180248181379L;
    }

    /**
     * Local Error subclass to check variety of error thrown.
     */
    private class JudgmentError extends Error {

        private static final long serialVersionUID = 634248373797713373L;
    }
}