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

public class SnapshotCommand {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    public static final SnapshotCommandSerializer serializer = new SnapshotCommandSerializer();

    public final String keyspace;

    public final String column_family;

    public final String snapshot_name;

    public final boolean clear_snapshot;

    public SnapshotCommand(String keyspace, String columnFamily, String snapshotName, boolean clearSnapshot) {
        this.keyspace = keyspace;
        this.column_family = columnFamily;
        this.snapshot_name = snapshotName;
        this.clear_snapshot = clearSnapshot;
    }

    public MessageOut createMessage() {
        return new MessageOut<SnapshotCommand>(MessagingService.Verb.SNAPSHOT, this, serializer);
    }

    @Override
    public String toString() {
        return "SnapshotCommand{" + "keyspace='" + keyspace + '\'' + ", column_family='" + column_family + '\'' + ", snapshot_name=" + snapshot_name + ", clear_snapshot=" + clear_snapshot + '}';
    }
}

class SnapshotCommandSerializer implements IVersionedSerializer<SnapshotCommand> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public void serialize(SnapshotCommand snapshot_command, DataOutputPlus out, int version) throws IOException {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(snapshot_command, snapshot_command.keyspace, "snapshot_command.keyspace").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(snapshot_command.keyspace);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(snapshot_command, snapshot_command.column_family, "snapshot_command.column_family").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(snapshot_command.column_family);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(snapshot_command, snapshot_command.snapshot_name, "snapshot_command.snapshot_name").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(snapshot_command.snapshot_name);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(snapshot_command, snapshot_command.clear_snapshot, "snapshot_command.clear_snapshot").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeBoolean(snapshot_command.clear_snapshot);
    }

    public SnapshotCommand deserialize(DataInput in, int version) throws IOException {
        String keyspace = in.readUTF();
        String column_family = in.readUTF();
        String snapshot_name = in.readUTF();
        boolean clear_snapshot = in.readBoolean();
        return new SnapshotCommand(keyspace, column_family, snapshot_name, clear_snapshot);
    }

    public long serializedSize(SnapshotCommand sc, int version) {
        return TypeSizes.NATIVE.sizeof(sc.keyspace) + TypeSizes.NATIVE.sizeof(sc.column_family) + TypeSizes.NATIVE.sizeof(sc.snapshot_name) + TypeSizes.NATIVE.sizeof(sc.clear_snapshot);
    }
}
