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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class DecimalSerializer implements TypeSerializer<BigDecimal> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final DecimalSerializer instance = new DecimalSerializer();

    public BigDecimal deserialize(ByteBuffer bytes) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        // do not consume the contents of the ByteBuffer
        bytes = bytes.duplicate();
        int scale = bytes.getInt();
        byte[] bibytes = new byte[bytes.remaining()];
        bytes.get(bibytes);
        BigInteger bi = new BigInteger(bibytes);
        return new BigDecimal(bi, scale);
    }

    public ByteBuffer serialize(BigDecimal value) {
        if (value == null) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.utils.ByteBufferUtil.class, org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER, "org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }
        BigInteger bi = value.unscaledValue();
        int scale = value.scale();
        byte[] bibytes = bi.toByteArray();
        ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
        bytes.putInt(scale);
        bytes.put(bibytes);
        bytes.rewind();
        return bytes;
    }

    public void validate(ByteBuffer bytes) throws MarshalException {
        // We at least store the scale.
        if (bytes.remaining() != 0 && bytes.remaining() < 4)
            throw new MarshalException(String.format("Expected 0 or at least 4 bytes (%d)", bytes.remaining()));
    }

    public String toString(BigDecimal value) {
        return value == null ? "" : value.toPlainString();
    }

    public Class<BigDecimal> getType() {
        return BigDecimal.class;
    }
}
