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
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
 * This message is sent back the truncate operation and basically specifies if
 * the truncate succeeded.
 */
public class TruncateResponse {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    public static final TruncateResponseSerializer serializer = new TruncateResponseSerializer();

    public final String keyspace;

    public final String columnFamily;

    public final boolean success;

    public TruncateResponse(String keyspace, String columnFamily, boolean success) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.success = success;
    }

    public MessageOut<TruncateResponse> createMessage() {
        return new MessageOut<TruncateResponse>(MessagingService.Verb.REQUEST_RESPONSE, this, serializer);
    }

    public static class TruncateResponseSerializer implements IVersionedSerializer<TruncateResponse> {

        private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

        public void serialize(TruncateResponse tr, DataOutputPlus out, int version) throws IOException {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(tr, tr.keyspace, "tr.keyspace").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeUTF(tr.keyspace);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(tr, tr.columnFamily, "tr.columnFamily").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeUTF(tr.columnFamily);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(tr, tr.success, "tr.success").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeBoolean(tr.success);
        }

        public TruncateResponse deserialize(DataInput in, int version) throws IOException {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            boolean success = in.readBoolean();
            return new TruncateResponse(keyspace, columnFamily, success);
        }

        public long serializedSize(TruncateResponse tr, int version) {
            return TypeSizes.NATIVE.sizeof(tr.keyspace) + TypeSizes.NATIVE.sizeof(tr.columnFamily) + TypeSizes.NATIVE.sizeof(tr.success);
        }
    }
}
