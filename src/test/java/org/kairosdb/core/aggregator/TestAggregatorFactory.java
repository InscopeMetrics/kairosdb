/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.aggregator;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.kairosdb.core.KairosRootConfig;
import org.kairosdb.core.annotation.Feature;
import org.kairosdb.core.annotation.FeatureComponent;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.processingstage.FeatureProcessingFactory;
import org.kairosdb.core.processingstage.metadata.FeatureProcessorMetadata;
import org.kairosdb.eventbus.EventBusConfiguration;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.plugin.Aggregator;

import java.util.HashMap;
import java.util.Map;

@Feature(
        name = "aggregators",
        label = "Test Aggregator"
)
public class TestAggregatorFactory implements FeatureProcessingFactory<Aggregator> {
    private final Injector injector;
    private final Map<String, Class<?>> aggregators = new HashMap<>();

    public TestAggregatorFactory() throws KairosDBException {
        addAggregator(SumAggregator.class);
        addAggregator(MinAggregator.class);
        addAggregator(MaxAggregator.class);
        addAggregator(AvgAggregator.class);
        addAggregator(StdAggregator.class);
        addAggregator(PercentileAggregator.class);
        addAggregator(DivideAggregator.class);
        addAggregator(FirstAggregator.class);
        addAggregator(LastAggregator.class);
        addAggregator(SaveAsAggregator.class);
        addAggregator(TrimAggregator.class);

        injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(DoubleDataPointFactory.class).to(DoubleDataPointFactoryImpl.class);
                bind(FilterEventBus.class).toInstance(new FilterEventBus(new EventBusConfiguration(new KairosRootConfig())));

                for (final Class<?> aggregator : aggregators.values()) {
                    bind(aggregator);
                }
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void addAggregator(final Class<?> agg) {
        final String name = agg.getAnnotation(FeatureComponent.class).name();
        aggregators.put(name, agg);
    }

    @Override
    public Aggregator createFeatureProcessor(final String name) {
        final Class<?> aggregator = aggregators.get(name);
        if (aggregator != null)
            return (Aggregator) injector.getInstance(aggregator);
        return null;
    }

    @Override
    public Class<Aggregator> getFeature() {
        return Aggregator.class;
    }

    @Override
    public ImmutableList<FeatureProcessorMetadata> getFeatureProcessorMetadata() {
        return ImmutableList.copyOf(new FeatureProcessorMetadata[]{});
    }
}
