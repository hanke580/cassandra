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
import java.net.InetAddress;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.NodePair;
import org.apache.cassandra.repair.RepairJobDesc;

/**
 * @since 2.0
 */
public class SyncComplete extends RepairMessage {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    public static final MessageSerializer serializer = new SyncCompleteSerializer();

    /**
     * nodes that involved in this sync
     */
    public final NodePair nodes;

    /**
     * true if sync success, false otherwise
     */
    public final boolean success;

    public SyncComplete(RepairJobDesc desc, NodePair nodes, boolean success) {
        super(Type.SYNC_COMPLETE, desc);
        this.nodes = nodes;
        this.success = success;
    }

    public SyncComplete(RepairJobDesc desc, InetAddress endpoint1, InetAddress endpoint2, boolean success) {
        super(Type.SYNC_COMPLETE, desc);
        this.nodes = new NodePair(endpoint1, endpoint2);
        this.success = success;
    }

    private static class SyncCompleteSerializer implements MessageSerializer<SyncComplete> {

        private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

        public void serialize(SyncComplete message, DataOutputPlus out, int version) throws IOException {
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
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(message, message.nodes, "message.nodes").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            NodePair.serializer.serialize(message.nodes, out, version);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(message, message.success, "message.success").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeBoolean(message.success);
        }

        public SyncComplete deserialize(DataInput in, int version) throws IOException {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            NodePair nodes = NodePair.serializer.deserialize(in, version);
            return new SyncComplete(desc, nodes, in.readBoolean());
        }

        public long serializedSize(SyncComplete message, int version) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(message, message.desc, "message.desc").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(message, message.nodes, "message.nodes").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            size += NodePair.serializer.serializedSize(message.nodes, version);
            size += TypeSizes.NATIVE.sizeof(message.success);
            return size;
        }
    }
}
