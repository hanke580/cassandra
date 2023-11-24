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
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.EmptySerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A type that only accept empty data.
 * It is only useful as a value validation type, not as a comparator since column names can't be empty.
 */
public class EmptyType extends AbstractType<Void> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final EmptyType instance = new EmptyType();

    // singleton
    private EmptyType() {
    }

    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return 0;
    }

    public String getString(ByteBuffer bytes) {
        return "";
    }

    public ByteBuffer fromString(String source) throws MarshalException {
        if (!source.isEmpty())
            throw new MarshalException(String.format("'%s' is not empty", source));
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException {
        if (!(parsed instanceof String))
            throw new MarshalException(String.format("Expected an empty string, but got: %s", parsed));
        if (!((String) parsed).isEmpty())
            throw new MarshalException(String.format("'%s' is not empty", parsed));
        return new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public TypeSerializer<Void> getSerializer() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.serializers.EmptySerializer.class, org.apache.cassandra.serializers.EmptySerializer.instance, "org.apache.cassandra.serializers.EmptySerializer.instance").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return EmptySerializer.instance;
    }
}
