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
package org.apache.cassandra.service.pager;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class PagingState {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public final ByteBuffer partitionKey;

    public final ByteBuffer cellName;

    public final int remaining;

    public PagingState(ByteBuffer partitionKey, ByteBuffer cellName, int remaining) {
        this.partitionKey = partitionKey == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : partitionKey;
        this.cellName = cellName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : cellName;
        this.remaining = remaining;
    }

    public static PagingState deserialize(ByteBuffer bytes) {
        if (bytes == null)
            return null;
        try {
            DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(bytes));
            ByteBuffer pk = ByteBufferUtil.readWithShortLength(in);
            ByteBuffer cn = ByteBufferUtil.readWithShortLength(in);
            int remaining = in.readInt();
            return new PagingState(pk, cn, remaining);
        } catch (IOException e) {
            throw new ProtocolException("Invalid value for the paging state");
        }
    }

    public ByteBuffer serialize() {
        try (DataOutputBuffer out = new DataOutputBufferFixed(serializedSize())) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.partitionKey, "this.partitionKey").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            ByteBufferUtil.writeWithShortLength(partitionKey, out);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.cellName, "this.cellName").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            ByteBufferUtil.writeWithShortLength(cellName, out);
            out.writeInt(remaining);
            return out.buffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int serializedSize() {
        return 2 + partitionKey.remaining() + 2 + cellName.remaining() + 4;
    }

    @Override
    public String toString() {
        return String.format("PagingState(key=%s, cellname=%s, remaining=%d", ByteBufferUtil.bytesToHex(partitionKey), ByteBufferUtil.bytesToHex(cellName), remaining);
    }
}
