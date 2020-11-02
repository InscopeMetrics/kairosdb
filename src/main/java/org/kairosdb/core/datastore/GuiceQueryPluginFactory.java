package org.kairosdb.core.datastore;

import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.annotation.PluginName;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bhawkins on 11/23/14.
 */
public class GuiceQueryPluginFactory implements QueryPluginFactory {
    private final Injector m_injector;
    private final Map<String, Class<QueryPlugin>> m_plugins = new HashMap<String, Class<QueryPlugin>>();

    @Inject
    @SuppressWarnings("unchecked")
    public GuiceQueryPluginFactory(final Injector injector) {
        m_injector = injector;
        final Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

        for (final Key<?> key : bindings.keySet()) {
            final Class<?> bindingClass = key.getTypeLiteral().getRawType();
            if (QueryPlugin.class.isAssignableFrom(bindingClass)) {
                final PluginName ann = bindingClass.getAnnotation(PluginName.class);
                if (ann == null)
                    throw new IllegalStateException("QueryPlugin class " + bindingClass.getName() +
                            " does not have required annotation " + PluginName.class.getName());

                m_plugins.put(ann.name(), (Class<QueryPlugin>) bindingClass);
            }
        }
    }

    @Override
    public QueryPlugin createQueryPlugin(final String name) {
        final Class<QueryPlugin> pluginClass = m_plugins.get(name);

        if (pluginClass == null)
            return (null);

        final QueryPlugin plugin = m_injector.getInstance(pluginClass);
        return (plugin);
    }
}
