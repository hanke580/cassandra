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
package org.apache.cassandra.serializers;

import org.apache.cassandra.utils.ByteBufferUtil;
import java.nio.ByteBuffer;

public class ShortSerializer implements TypeSerializer<Short> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final ShortSerializer instance = new ShortSerializer();

    public Short deserialize(ByteBuffer bytes) {
        return bytes.remaining() == 0 ? null : ByteBufferUtil.toShort(bytes);
    }

    public ByteBuffer serialize(Short value) {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.utils.ByteBufferUtil.class, org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER, "org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value.shortValue());
    }

    public void validate(ByteBuffer bytes) throws MarshalException {
        if (bytes.remaining() != 2)
            throw new MarshalException(String.format("Expected 2 bytes for a smallint (%d)", bytes.remaining()));
    }

    public String toString(Short value) {
        return value == null ? "" : String.valueOf(value);
    }

    public Class<Short> getType() {
        return Short.class;
    }
}
