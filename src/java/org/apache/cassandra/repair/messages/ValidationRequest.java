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
package org.apache.cassandra.repair.messages;

import java.io.DataInput;
import java.io.IOException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;

/**
 * ValidationRequest
 *
 * @since 2.0
 */
public class ValidationRequest extends RepairMessage {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    public static MessageSerializer serializer = new ValidationRequestSerializer();

    public final int gcBefore;

    public ValidationRequest(RepairJobDesc desc, int gcBefore) {
        super(Type.VALIDATION_REQUEST, desc);
        this.gcBefore = gcBefore;
    }

    @Override
    public String toString() {
        return "ValidationRequest{" + "gcBefore=" + gcBefore + "} " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ValidationRequest that = (ValidationRequest) o;
        return gcBefore == that.gcBefore;
    }

    @Override
    public int hashCode() {
        return gcBefore;
    }

    public static class ValidationRequestSerializer implements MessageSerializer<ValidationRequest> {

        private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

        public void serialize(ValidationRequest message, DataOutputPlus out, int version) throws IOException {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(message, message.desc, "message.desc").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(message, message.gcBefore, "message.gcBefore").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeInt(message.gcBefore);
        }

        public ValidationRequest deserialize(DataInput dis, int version) throws IOException {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(dis, version);
            return new ValidationRequest(desc, dis.readInt());
        }

        public long serializedSize(ValidationRequest message, int version) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(message, message.desc, "message.desc").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += TypeSizes.NATIVE.sizeof(message.gcBefore);
            return size;
        }
    }
}
