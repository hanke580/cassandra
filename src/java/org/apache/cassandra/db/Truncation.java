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
 * A truncate operation descriptor
 */
public class Truncation {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    public static final IVersionedSerializer<Truncation> serializer = new TruncationSerializer();

    public final String keyspace;

    public final String columnFamily;

    public Truncation(String keyspace, String columnFamily) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
    }

    public MessageOut<Truncation> createMessage() {
        return new MessageOut<Truncation>(MessagingService.Verb.TRUNCATE, this, serializer);
    }

    public String toString() {
        return "Truncation(" + "keyspace='" + keyspace + '\'' + ", cf='" + columnFamily + "\')";
    }
}

class TruncationSerializer implements IVersionedSerializer<Truncation> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public void serialize(Truncation t, DataOutputPlus out, int version) throws IOException {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(t, t.keyspace, "t.keyspace").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(t.keyspace);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(t, t.columnFamily, "t.columnFamily").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(t.columnFamily);
    }

    public Truncation deserialize(DataInput in, int version) throws IOException {
        String keyspace = in.readUTF();
        String columnFamily = in.readUTF();
        return new Truncation(keyspace, columnFamily);
    }

    public long serializedSize(Truncation truncation, int version) {
        return TypeSizes.NATIVE.sizeof(truncation.keyspace) + TypeSizes.NATIVE.sizeof(truncation.columnFamily);
    }
}
