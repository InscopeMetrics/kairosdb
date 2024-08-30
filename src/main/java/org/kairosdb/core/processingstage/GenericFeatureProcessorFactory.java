package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binding;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;
import org.kairosdb.core.processingstage.metadata.FeaturePropertyMetadata;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jakarta.validation.constraints.NotNull;

import static org.kairosdb.core.annotation.AnnotationUtils.getPropertyMetadata;

public abstract class GenericFeatureProcessorFactory<FEATURE> implements FeatureProcessingFactory<FEATURE> {
    protected Map<String, Class<FEATURE>> featureProcessors = new HashMap<>();
    protected List<FeatureProcessorMetadata> featureProcessorMetadata = new ArrayList<>();
    protected Injector injector;
    private final Class<FEATURE> featureClass;

    /**
     * Constructor of a generic class to easily generate a feature processing factory.
     *
     * @param injector     Guice {@link Injector} instance needed for binding
     * @param featureClass feature processor class
     */
    @SuppressWarnings("unchecked")
    protected GenericFeatureProcessorFactory(@NotNull final Injector injector, @NotNull final Class<FEATURE> featureClass)
            throws InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IllegalAccessException {
        this.injector = injector;
        this.featureClass = featureClass;
        final Map<Key<?>, Binding<?>> bindings = injector.getAllBindings();

        for (final Key<?> key : bindings.keySet()) {
            final Class<?> bindingClass = key.getTypeLiteral().getRawType();

            if (featureClass.isAssignableFrom(bindingClass)) {
                final FeatureComponent annotation = bindingClass.getAnnotation(FeatureComponent.class);
                if (annotation == null)
                    throw new IllegalStateException("Processor class " + bindingClass.getName() +
                            " does not have required annotation " + FeatureComponent.class.getName());

                featureProcessors.put(annotation.name(), (Class<FEATURE>) bindingClass);
                final List<FeaturePropertyMetadata> properties = getPropertyMetadata(bindingClass);
                featureProcessorMetadata.add(new FeatureProcessorMetadata(annotation.name(), labelizeComponent(annotation), annotation.description(), properties));
            }
        }
        featureProcessorMetadata.sort(Comparator.comparing(FeatureProcessorMetadata::getName));
    }

    @Override
    public Class<FEATURE> getFeature() {
        return featureClass;
    }

    @Override
    public ImmutableList<FeatureProcessorMetadata> getFeatureProcessorMetadata() {
        return new ImmutableList.Builder<FeatureProcessorMetadata>().addAll(featureProcessorMetadata).build();
    }

    @Override
    public FEATURE createFeatureProcessor(final String name) {
        final Class<FEATURE> processClass = featureProcessors.get(name);

        if (processClass == null)
            return (null);
        return (injector.getInstance(processClass));
    }

    private String labelizeComponent(final FeatureComponent annotation) {
        if (!annotation.label().isEmpty())
            return annotation.label();

        final StringBuilder label = new StringBuilder();
        for (final String word : annotation.name().toLowerCase().split("_")) {
            label.append(word.substring(0, 1).toUpperCase());
            label.append(word.substring(1));
            label.append(" ");
        }
        return label.toString().trim();
    }
}
