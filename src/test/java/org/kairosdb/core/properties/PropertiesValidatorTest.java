/*
 * Copyright 2020 Dropbox
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kairosdb.core.properties;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.reporting.MetricReporterService;
import org.kairosdb.datastore.cassandra.CassandraConfiguration;

import java.util.Optional;

/**
 * Tests for {@link PropertiesValidator}.
 *
 * @author Max Wittek (https://github.com/wittekm)
 */
public final class PropertiesValidatorTest {
    private static final int SOME_POSITIVE_INT = 1234;
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void testWhenNothingSpecifiedValidatorSucceeds() throws KairosDBException {
        ValidatorArgs args = new ValidatorArgs();

        validatorWithInjection(args).validate();
    }

    @Test
    public void testWhenForceAndTtlValidatorSucceeds() throws KairosDBException {
        ValidatorArgs args = new ValidatorArgs();
        args.forceDefaultDatapointTtl = Optional.of(true);
        args.cassandraDatapointTtl = Optional.of(100);

        validatorWithInjection(args).validate();
    }

    @Test
    public void testWhenOnlyAlignIsOnWithoutReporterTtlValidatorFails() throws KairosDBException {
        exceptionRule.expect(KairosDBException.class);
        exceptionRule.expectMessage("you must set kairosdb.reporter.ttl to a positive value");

        ValidatorArgs args = new ValidatorArgs();
        args.alignDatapointTtlWithTimestamp = Optional.of(true);

        validatorWithInjection(args).validate();
    }

    @Test
    public void testWhenForceTtlIsOnWithoutCassandraTtlValidatorFails() throws KairosDBException {
        exceptionRule.expect(KairosDBException.class);
        exceptionRule.expectMessage("you must set kairosdb.datastore.cassandra.datapoint_ttl to a positive value");

        ValidatorArgs args = new ValidatorArgs();
        args.forceDefaultDatapointTtl = Optional.of(true);

        validatorWithInjection(args).validate();
    }

    @Test
    public void testWhenAlignOffValidatorSucceeds() throws KairosDBException {
        ValidatorArgs args = new ValidatorArgs();
        args.reporterTtl = Optional.of(SOME_POSITIVE_INT);
        args.cassandraDatapointTtl = Optional.of(SOME_POSITIVE_INT);

        validatorWithInjection(args).validate();
    }

    @Test
    public void testWhenAlignIsOnAndBothTtlsSpecifiedValidatorSucceeds() throws KairosDBException {
        ValidatorArgs args = new ValidatorArgs();
        args.alignDatapointTtlWithTimestamp = Optional.of(true);
        args.reporterTtl = Optional.of(SOME_POSITIVE_INT);
        args.cassandraDatapointTtl = Optional.of(SOME_POSITIVE_INT);

        validatorWithInjection(args).validate();
    }

    private PropertiesValidator validatorWithInjection(final ValidatorArgs args) {
        final Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                args.alignDatapointTtlWithTimestamp.ifPresent(val -> bind(Boolean.TYPE)
                        .annotatedWith(Names.named(CassandraConfiguration.ALIGN_DATAPOINT_TTL_WITH_TIMESTAMP))
                        .toInstance(val));
                args.reporterTtl.ifPresent(val -> bind(Integer.TYPE)
                        .annotatedWith(Names.named(MetricReporterService.REPORTER_TTL))
                        .toInstance(val));
                args.cassandraDatapointTtl.ifPresent(val -> bind(Integer.TYPE)
                        .annotatedWith(Names.named(CassandraConfiguration.DATAPOINT_TTL))
                        .toInstance(val));
                args.forceDefaultDatapointTtl.ifPresent(val -> bind(Boolean.TYPE)
                        .annotatedWith(Names.named(CassandraConfiguration.FORCE_DEFAULT_DATAPOINT_TTL))
                        .toInstance(val));
            }
        });
        return injector.getInstance(PropertiesValidator.class);
    }

    private static class ValidatorArgs {
        // a struct! in java? sacrilege
        Optional<Boolean> alignDatapointTtlWithTimestamp = Optional.empty();
        Optional<Integer> reporterTtl = Optional.empty();
        Optional<Integer> cassandraDatapointTtl = Optional.empty();
        Optional<Boolean> forceDefaultDatapointTtl = Optional.empty();
    }
}
