package org.kairosdb.core.processingstage;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.annotatedAggregator.AAggregator;
import org.kairosdb.core.processingstage.metadata.FeatureProcessingMetadata;
import org.kairosdb.plugin.Aggregator;
import org.kairosdb.plugin.GroupBy;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.kairosdb.core.processingstage.GenericFeatureProcessorFactoryTest.assertQueryProcessors;
import static org.kairosdb.core.processingstage.GenericFeatureProcessorFactoryTest.factory_valid_metadata_generator;

public class GenericProcessingChainTest {
    private static FeatureProcessor processingChain;

    @BeforeClass
    public static void chain_generation_valid()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Injector injector = Guice.createInjector(binder -> binder.bind(AAggregator.class));

        final List<FeatureProcessingFactory<?>> list = new ArrayList<FeatureProcessingFactory<?>>();
        list.add(new AggregatorFactory(injector));
        GenericProcessingChainTest.processingChain = new TestKairosDBProcessor(list);
    }

    static FeatureProcessingMetadata[] chain_valid_metadata_generator()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        return new FeatureProcessingMetadata[]{
                new FeatureProcessingMetadata(
                        "aggregators",
                        "Aggregator",
                        Arrays.asList(factory_valid_metadata_generator())
                )
        };
    }

    static void assertQueryProcessorFactories(final ImmutableList<FeatureProcessingMetadata> expectedProcessingChain,
                                              final ImmutableList<FeatureProcessingMetadata> actualProcessingChain) {
        assertEquals("Feature metadata size don't match", expectedProcessingChain.size(), actualProcessingChain.size());
        for (int i = 0; i < actualProcessingChain.size(); i++) {
            final FeatureProcessingMetadata expectedQueryProcessorStage = expectedProcessingChain.get(i);
            final FeatureProcessingMetadata actualQueryProcessorStage = actualProcessingChain.get(i);
            assertEquals("Feature metadata name don't match", expectedQueryProcessorStage.getName(), actualQueryProcessorStage.getName());
            assertEquals("Feature metadata label don't match", expectedQueryProcessorStage.getLabel(), actualQueryProcessorStage.getLabel());
            assertQueryProcessors(expectedQueryProcessorStage.getProperties(), actualQueryProcessorStage.getProperties());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void chain_generation_empty_list()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        new TestKairosDBProcessor(new ArrayList<>());
    }

    @Test(expected = NullPointerException.class)
    public void chain_generation_null_list() {
        new TestKairosDBProcessor(null);
    }

    @Test
    public void chain_getter_factory_with_name() {
        final FeatureProcessingFactory<?> factory = GenericProcessingChainTest.processingChain.getFeatureProcessingFactory(Aggregator.class);
        assertEquals("Invalid type of FeatureProcessingFactory", AggregatorFactory.class, factory.getClass());
    }

    @Test
    public void chain_getter_factory_with_name_failure() {
        final FeatureProcessingFactory<?> factory = GenericProcessingChainTest.processingChain.getFeatureProcessingFactory(GroupBy.class);
        assertEquals("Invalid type of FeatureProcessingFactory", null, factory);
    }

    @Test
    public void chain_getter_factory_with_class() {
        final FeatureProcessingFactory<?> factory = GenericProcessingChainTest.processingChain.getFeatureProcessingFactory("aggregators");
        assertEquals("Invalid type of FeatureProcessingFactory", AggregatorFactory.class, factory.getClass());
    }

    @Test
    public void chain_getter_factory_with_class_failure() {
        final FeatureProcessingFactory<?> factory = GenericProcessingChainTest.processingChain.getFeatureProcessingFactory("groupby");
        assertEquals("Invalid type of FeatureProcessingFactory", null, factory);
    }

    @Test
    public void chain_getter_metadata()
            throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        assertQueryProcessorFactories(
                ImmutableList.copyOf(chain_valid_metadata_generator()),
                GenericProcessingChainTest.processingChain.getFeatureProcessingMetadata()
        );
    }
}
