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

package org.kairosdb.util;

import com.google.gson.JsonElement;
import org.kairosdb.core.http.rest.json.ValidationErrors;

public class Validator {
    private Validator() {
    }


    public static void validateNotNullOrEmpty(final String name, final String value) throws ValidationException {
        final ValidationErrors errors = new ValidationErrors();
        if (!isNotNullOrEmpty(errors, name, value))
            throw new ValidationException(errors.getFirstError());
    }

    public static void validateMin(final String name, final long value, final long minValue) throws ValidationException {
        final ValidationErrors errors = new ValidationErrors();
        if (!isGreaterThanOrEqualTo(errors, name, value, minValue))
            throw new ValidationException(errors.getFirstError());
    }


    public static boolean isNotNullOrEmpty(final ValidationErrors validationErrors, final Object name, final String value) {
        if (value == null) {
            validationErrors.addErrorMessage(name + " may not be null.");
            return false;
        }
        if (value.isEmpty()) {
            validationErrors.addErrorMessage(name + " may not be empty.");
            return false;
        }

        return true;
    }

    public static boolean isNotNull(final ValidationErrors validationErrors, final Object name, final Object value) {
        if (value == null) {
            validationErrors.addErrorMessage(name + " may not be null.");
            return false;
        }
        return true;
    }

    public static boolean isNotNullOrEmpty(final ValidationErrors validationErrors, final Object name, final JsonElement value) {
        if (value == null) {
            validationErrors.addErrorMessage(name + " may not be null.");
            return false;
        }
        if (value.isJsonNull()) {
            validationErrors.addErrorMessage(name + " may not be empty.");
            return false;
        }
        if (value.isJsonArray() && value.getAsJsonArray().size() < 1) {
            validationErrors.addErrorMessage(name + " may not be an empty array.");
            return false;
        }
        if (!value.isJsonObject() && value.getAsString().isEmpty()) {
            validationErrors.addErrorMessage(name + " may not be empty.");
            return false;
        }

        return true;
    }

    public static boolean isGreaterThanOrEqualTo(final ValidationErrors validationErrors, final Object name, final long value, final long minValue) {
        if (value < minValue) {
            validationErrors.addErrorMessage(name + " must be greater than or equal to " + minValue + ".");
            return false;
        }
        return true;
    }

}