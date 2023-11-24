/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class FloatType extends AbstractType<Float> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final FloatType instance = new FloatType();

    // singleton
    FloatType() {
    }

    public boolean isEmptyValueMeaningless() {
        return true;
    }

    public int compare(ByteBuffer o1, ByteBuffer o2) {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;
        return compose(o1).compareTo(compose(o2));
    }

    public ByteBuffer fromString(String source) throws MarshalException {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        try {
            float f = Float.parseFloat(source);
            return ByteBufferUtil.bytes(f);
        } catch (NumberFormatException e1) {
            throw new MarshalException(String.format("Unable to make float from '%s'", source), e1);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException {
        try {
            if (parsed instanceof String)
                return new Constants.Value(fromString((String) parsed));
            else
                return new Constants.Value(getSerializer().serialize(((Number) parsed).floatValue()));
        } catch (ClassCastException exc) {
            throw new MarshalException(String.format("Expected a float value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, int protocolVersion) {
        return getSerializer().deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type() {
        return CQL3Type.Native.FLOAT;
    }

    public TypeSerializer<Float> getSerializer() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.serializers.FloatSerializer.class, org.apache.cassandra.serializers.FloatSerializer.instance, "org.apache.cassandra.serializers.FloatSerializer.instance").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return FloatSerializer.instance;
    }
}
