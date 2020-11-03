package org.kairosdb.core.processingstage;

import java.util.List;

public class TestKairosDBProcessor extends GenericFeatureProcessor {
    public TestKairosDBProcessor(final List<FeatureProcessingFactory<?>> processingChain) {
        super(processingChain);
    }
}
