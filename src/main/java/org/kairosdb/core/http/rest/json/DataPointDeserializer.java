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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.kairosdb.core.http.rest.BeanValidationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;

public class DataPointDeserializer extends JsonDeserializer<List<DataPointRequest>> {
    private static final Validator VALIDATOR = Validation.byProvider(ApacheValidationProvider.class).configure().buildValidatorFactory().getValidator();

    @Override
    public List<DataPointRequest> deserialize(final JsonParser parser, final DeserializationContext deserializationContext) throws IOException {
        final List<DataPointRequest> datapoints = new ArrayList<>();

        JsonToken token = parser.nextToken();
        if (token != JsonToken.START_ARRAY) {
            deserializationContext.reportWrongTokenException(
                    DataPointRequest.class,
                    JsonToken.START_ARRAY,
                    "Invalid data point syntax.");
        }

        while (token != null && token != JsonToken.END_ARRAY) {
            parser.nextToken();
            final long timestamp = parser.getLongValue();

            parser.nextToken();
            final String value = parser.getText();

            final DataPointRequest dataPointRequest = new DataPointRequest(timestamp, value);

            validateObject(dataPointRequest);
            datapoints.add(dataPointRequest);

            token = parser.nextToken();
            if (token != JsonToken.END_ARRAY) {
                deserializationContext.reportWrongTokenException(
                        DataPointRequest.class,
                        JsonToken.END_ARRAY,
                        "Invalid data point syntax.");
            }

            token = parser.nextToken();
        }

        return datapoints;
    }

    private void validateObject(final Object request) throws BeanValidationException {
        // validate object using the bean validation framework
        final Set<ConstraintViolation<Object>> violations = VALIDATOR.validate(request);
        if (!violations.isEmpty()) {
            throw new BeanValidationException(violations);
        }
    }
}

