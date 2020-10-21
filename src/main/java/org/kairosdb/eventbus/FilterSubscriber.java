/*
 * Copyright (C) 2014 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.kairosdb.eventbus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A subscriber method on a filter object. Uses an executor that executes in the same thread.
 * <p>
 * <p>Two subscribers are equivalent when they refer to the same method on the same object (not
 * class). This property is used to ensure that no subscriber method is registered more than once.
 */
class FilterSubscriber implements Comparable<FilterSubscriber> {
    /**
     * FilterSubscriber method.
     */
    protected final Method method;
    /**
     * The object with the subscriber method.
     */
    @VisibleForTesting
    final Object target;
    private final int priority;
    /**
     * Executor to use for dispatching events to this subscriber.
     */
    private final Executor executor = MoreExecutors.directExecutor();
    /**
     * The event bus this subscriber belongs to.
     */
    private final FilterEventBus bus;

    private FilterSubscriber(final FilterEventBus bus, final Object target, final Method method, final int priority) {
        this.bus = bus;
        this.target = checkNotNull(target);
        this.method = method;
        this.priority = priority;
        method.setAccessible(true);
        checkArgument(priority >= 0 && priority <= 100, "Priority must be between 0 and 100 inclusive");
    }

    /**
     * Creates a {@code FilterSubscriber} for {@code method} on {@code listener}.
     */
    static FilterSubscriber create(final FilterEventBus bus, final Object listener, final Method method, final int priority) {
        return method.getReturnType().getName().equals("void")
                ? new NonFilterSubscriber(bus, listener, method, priority)
                : new FilterSubscriber(bus, listener, method, priority);
    }

    public int getPriority() {
        return priority;
    }

    final Object dispatchEvent(final Object event) {
        try {
            return invokeSubscriberMethod(event);
        } catch (final IllegalArgumentException e) {
            throw new Error("Method rejected target/argument: " + event, e);
        } catch (final IllegalAccessException e) {
            throw new Error("Method became inaccessible: " + event, e);
        } catch (final InvocationTargetException e) {
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }

            bus.handleSubscriberException(e.getCause(), context(event));
            return null;
        }
		/*catch (InvocationTargetException e)
		{
			bus.handleSubscriberException(e.getCause(), context(event));
			return null;
		}*/
    }

    /**
     * Invokes the subscriber method. This method can be overridden to make the invocation
     * synchronized.
     */
    @VisibleForTesting
    Object invokeSubscriberMethod(final Object event) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(target, checkNotNull(event));
    }

    /**
     * Gets the context for the given event.
     */
    private SubscriberExceptionContext context(final Object event) {
        return new SubscriberExceptionContext(bus, event, target, method);
    }

    @Override
    public final int hashCode() {
        return (31 + method.hashCode()) * 31 + System.identityHashCode(target);
    }

    @Override
    public final boolean equals(@Nullable final Object obj) {
        if (obj instanceof FilterSubscriber) {
            final FilterSubscriber that = (FilterSubscriber) obj;
            // Use == so that different equal instances will still receive events.
            // We only guard against the case that the same object is registered
            // multiple times
            return target == that.target && method.equals(that.method);
        }
        return false;
    }

    @Override
    public int compareTo(final FilterSubscriber o) {
        if (priority < o.priority)
            return -1;
        else if (priority > o.priority)
            return 1;
        else {
            final String thisName = System.identityHashCode(target) + method.getName();
            final String thatName = System.identityHashCode(o.target) + o.method.getName();

            return thisName.compareTo(thatName);
        }
    }

    private static class NonFilterSubscriber extends FilterSubscriber {
        private NonFilterSubscriber(final FilterEventBus bus, final Object target, final Method method, final int priority) {
            super(bus, target, method, priority);
        }

        @Override
        Object invokeSubscriberMethod(final Object event) throws InvocationTargetException, IllegalAccessException {
            method.invoke(target, checkNotNull(event));
            return event;
        }
    }

}
