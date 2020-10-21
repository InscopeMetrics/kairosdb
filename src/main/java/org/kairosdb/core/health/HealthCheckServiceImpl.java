package org.kairosdb.core.health;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HealthCheckServiceImpl implements HealthCheckService {
    private final List<HealthStatus> checks = new ArrayList<>();

    @Inject
    public HealthCheckServiceImpl(final Injector injector) {
        checkNotNull(injector);

        final Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

        for (final Key<?> key : bindings.keySet()) {
            final Class<?> bindingClass = key.getTypeLiteral().getRawType();
            if (HealthStatus.class.isAssignableFrom(bindingClass)) {
                checks.add((HealthStatus) injector.getInstance(bindingClass));
            }
        }
    }

    @Override
    public List<HealthStatus> getChecks() {
        return Collections.unmodifiableList(checks);
    }
}
