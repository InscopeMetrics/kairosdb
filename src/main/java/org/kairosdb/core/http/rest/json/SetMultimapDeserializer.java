package org.kairosdb.core.http.rest.json;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.Map;

public class SetMultimapDeserializer implements JsonDeserializer<SetMultimap<String, String>> {
    @Override
    public SetMultimap<String, String> deserialize(final JsonElement json, final Type typeOfT, final JsonDeserializationContext context) throws JsonParseException {
        final SetMultimap<String, String> map = HashMultimap.create();

        final JsonObject filters = json.getAsJsonObject();
        for (final Map.Entry<String, JsonElement> filter : filters.entrySet()) {
            final String name = filter.getKey();
            final JsonArray values = ((JsonArray) filter.getValue());
            for (final JsonElement value : values) {
                map.put(name, value.getAsString());
            }
        }

        return map;
    }
}
