package org.kairosdb.core.annotation;

import com.google.common.base.Defaults;
import org.apache.commons.lang3.ClassUtils;
import org.kairosdb.core.processingstage.metadata.FeaturePropertyMetadata;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class AnnotationUtils {
    public static List<FeaturePropertyMetadata> getPropertyMetadata(final Class<?> clazz) throws ClassNotFoundException {
        checkNotNull(clazz, "class cannot be null");

        final List<FeaturePropertyMetadata> properties = new ArrayList<>();
        final Field[] fields = clazz.getDeclaredFields();
        for (final Field field : fields) {
            if (field.getAnnotation(FeatureProperty.class) != null) {
                String type = getType(field);
                String options = null;
                if (field.getType().isEnum()) {
                    options = getEnumAsString(field.getType());
                    type = "enum";
                }

                final FeatureProperty property = field.getAnnotation(FeatureProperty.class);
                properties.add(new FeaturePropertyMetadata(field.getName(), type, options,
                        isEmpty(property.default_value()) ? getDefaultValue(field) : property.default_value(),
                        property));
            }

            final FeatureCompoundProperty annotation = field.getAnnotation(FeatureCompoundProperty.class);
            if (annotation != null) {
                properties.add(new FeaturePropertyMetadata(field.getName(), annotation, getPropertyMetadata(field.getType())));
            }
        }

        if (clazz.getSuperclass() != null) {
            properties.addAll(getPropertyMetadata(clazz.getSuperclass()));
        }

        //noinspection Convert2Lambda
        properties.sort(new Comparator<FeaturePropertyMetadata>() {
            @Override
            public int compare(final FeaturePropertyMetadata o1, final FeaturePropertyMetadata o2) {
                return o1.getLabel().compareTo(o2.getLabel());
            }
        });

        return properties;
    }

    private static String getEnumAsString(final Class<?> type) {
        final StringBuilder builder = new StringBuilder();
        final Field[] declaredFields = type.getDeclaredFields();
        for (final Field declaredField : declaredFields) {
            if (declaredField.isEnumConstant()) {
                if (builder.length() > 0) {
                    builder.append(',');
                }
                builder.append(declaredField.getName());
            }
        }

        return builder.toString();
    }

    private static String getType(final Field field) {
        if (Collection.class.isAssignableFrom(field.getType()) || field.getType().isArray()) {
            return "array";
        }
        return field.getType().getSimpleName();
    }

    private static String getDefaultValue(final Field field)
            throws ClassNotFoundException {
        if (field.getType().isAssignableFrom(String.class)) {
            return "";
        } else if (Collection.class.isAssignableFrom(field.getType()) || field.getType().isArray()) {
            return "[]";
        } else {
            return String.valueOf(Defaults.defaultValue(ClassUtils.getClass(field.getType().getSimpleName())));
        }
    }
}
