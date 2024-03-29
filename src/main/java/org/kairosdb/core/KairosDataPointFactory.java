/*
 * Copyright 2016 KairosDB Authors
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

package org.kairosdb.core;

import com.google.gson.JsonElement;
import org.kairosdb.core.datapoints.DataPointFactory;
import org.kairosdb.util.KDataInput;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bhawkins
 * Date: 9/10/13
 * Time: 8:55 AM
 * To change this template use File | Settings | File Templates.
 */
public interface KairosDataPointFactory {
    DataPoint createDataPoint(String type, long timestamp, JsonElement json) throws IOException;

    DataPoint createDataPoint(String type, long timestamp, KDataInput buffer) throws IOException;

    //public DataPoint createDataPoint(byte type, long timestamp, ByteBuffer buffer);

    //public byte getTypeByte(String type);

    DataPointFactory getFactoryForType(String type);

    DataPointFactory getFactoryForDataStoreType(String dataStoreType);

    String getGroupType(String datastoreType);

    boolean isRegisteredType(String type);
}
