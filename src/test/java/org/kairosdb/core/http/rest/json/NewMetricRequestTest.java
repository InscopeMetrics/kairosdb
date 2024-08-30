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
package org.kairosdb.core.http.rest.json;

import org.junit.Test;
import org.kairosdb.testing.BeanValidationHelper;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import jakarta.validation.ConstraintViolation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class NewMetricRequestTest {
    @Test
    public void testNullNameInvalid() {
        final NewMetricRequest request = new NewMetricRequest(null, new HashMap<String, String>());
        request.addDataPoint(new DataPointRequest(5, "value"));
        final Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
        final List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

        assertThat(violationMessages.size(), equalTo(2));
        assertThat(violationMessages.get(0), equalTo("name may not be null"));
        assertThat(violationMessages.get(1), equalTo("name must not be empty"));

    }

    @Test
    public void testEmptyNameInvalid() {
        final NewMetricRequest request = new NewMetricRequest("", new HashMap<String, String>());
        request.addDataPoint(new DataPointRequest(5, "value"));
        final Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
        final List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

        assertThat(violationMessages.size(), equalTo(1));
        assertThat(violationMessages.get(0), equalTo("name must not be empty"));

    }

    @Test
    public void testNullValueInvalid() {
        final NewMetricRequest request = new NewMetricRequest("metric1", new HashMap<String, String>());
        request.addDataPoint(new DataPointRequest(5, null));

        final Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
        final List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

        assertThat(violationMessages.size(), equalTo(2));
        assertThat(violationMessages.get(0), equalTo("datapoints[0].value may not be null"));
        assertThat(violationMessages.get(1), equalTo("datapoints[0].value must not be empty"));

    }

    @Test
    public void testEmptyValueInvalid() {
        final NewMetricRequest request = new NewMetricRequest("metric1", new HashMap<String, String>());
        request.addDataPoint(new DataPointRequest(5, ""));

        final Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
        final List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

        assertThat(violationMessages.size(), equalTo(1));
        assertThat(violationMessages.get(0), equalTo("datapoints[0].value must not be empty"));

    }

    @Test
    public void testTimestampZeroInvalid() {
        final NewMetricRequest request = new NewMetricRequest("metric1", new HashMap<String, String>());
        request.addDataPoint(new DataPointRequest(0, "value"));

        final Set<ConstraintViolation<NewMetricRequest>> violations = BeanValidationHelper.VALIDATOR.validate(request);
        final List<String> violationMessages = BeanValidationHelper.messagesFor(violations);

        assertThat(violationMessages.size(), equalTo(1));
        assertThat(violationMessages.get(0), equalTo("datapoints[0].timestamp must be greater than or equal to 1"));

    }
}