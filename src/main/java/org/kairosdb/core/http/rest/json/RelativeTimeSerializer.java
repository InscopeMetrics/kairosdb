package org.kairosdb.core.http.rest.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class RelativeTimeSerializer implements JsonSerializer<RelativeTime> {
    @Override
    public JsonElement serialize(final RelativeTime relativeTime, final Type type, final JsonSerializationContext jsonSerializationContext) {
        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("value", new JsonPrimitive(relativeTime.getValue()));
        jsonObject.add("unit", new JsonPrimitive(relativeTime.getUnit().name().toLowerCase()));

        return jsonObject;
    }
}
