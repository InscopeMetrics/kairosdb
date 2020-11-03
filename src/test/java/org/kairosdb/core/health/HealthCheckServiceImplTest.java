package org.kairosdb.core.health;

import com.codahale.metrics.health.HealthCheck;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class HealthCheckServiceImplTest {

    @Test(expected = NullPointerException.class)
    public void testConstructorNullInjectorInvalid() {
        new HealthCheckServiceImpl(null);
    }

    @Test
    public void testGetChecks() {
        final Injector injector = Guice.createInjector(new TestModule());
        final HealthCheckServiceImpl checkService = new HealthCheckServiceImpl(injector);

        final List<HealthStatus> checks = checkService.getChecks();

        assertThat(checks.size(), equalTo(2));
        assertThat(checks, hasItems(new HealthStatus1(), new HealthStatus2()));
    }

    private static class HealthStatus1 implements HealthStatus {
        private final String name = getClass().getSimpleName();

        @Override
        public String getName() {
            return name;
        }

        @Override
        public HealthCheck.Result execute() {
            return null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final HealthStatus1 that = (HealthStatus1) o;

            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    private static class HealthStatus2 implements HealthStatus {
        private final String name = getClass().getSimpleName();

        @Override
        public String getName() {
            return name;
        }

        @Override
        public HealthCheck.Result execute() {
            return null;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final HealthStatus2 that = (HealthStatus2) o;

            return !(name != null ? !name.equals(that.name) : that.name != null);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    private static class NotHealthStatus {
    }

    private class TestModule extends AbstractModule {

        @Override
        protected void configure() {
            bind(HealthStatus1.class);
            bind(HealthStatus2.class);
            bind(NotHealthStatus.class);
        }
    }
}