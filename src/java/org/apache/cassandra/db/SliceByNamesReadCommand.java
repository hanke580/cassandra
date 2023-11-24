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

import java.io.*;
import java.nio.ByteBuffer;
import com.google.common.base.Objects;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceByNamesReadCommand extends ReadCommand {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    static final SliceByNamesReadCommandSerializer serializer = new SliceByNamesReadCommandSerializer();

    public final NamesQueryFilter filter;

    public SliceByNamesReadCommand(String keyspaceName, ByteBuffer key, String cfName, long timestamp, NamesQueryFilter filter) {
        super(keyspaceName, key, cfName, timestamp, Type.GET_BY_NAMES);
        this.filter = filter;
    }

    public ReadCommand copy() {
        return new SliceByNamesReadCommand(ksName, key, cfName, timestamp, filter).setIsDigestQuery(isDigestQuery());
    }

    public Row getRow(Keyspace keyspace) {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return keyspace.getRow(new QueryFilter(dk, cfName, filter, timestamp));
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("ksName", ksName).add("cfName", cfName).add("key", ByteBufferUtil.bytesToHex(key)).add("filter", filter).add("timestamp", timestamp).toString();
    }

    public IDiskAtomFilter filter() {
        return filter;
    }
}

class SliceByNamesReadCommandSerializer implements IVersionedSerializer<ReadCommand> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public void serialize(ReadCommand cmd, DataOutputPlus out, int version) throws IOException {
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        out.writeBoolean(command.isDigestQuery());
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(command, command.ksName, "command.ksName").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(command.ksName);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(command, command.key, "command.key").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        ByteBufferUtil.writeWithShortLength(command.key, out);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(command, command.cfName, "command.cfName").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(command.cfName);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.timestamp, "cmd.timestamp").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeLong(cmd.timestamp);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.ksName, "cmd.ksName").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.cfName, "cmd.cfName").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.ksName, cmd.cfName);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(command, command.filter, "command.filter").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        metadata.comparator.namesQueryFilterSerializer().serialize(command.filter, out, version);
    }

    public ReadCommand deserialize(DataInput in, int version) throws IOException {
        boolean isDigest = in.readBoolean();
        String keyspaceName = in.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
        String cfName = in.readUTF();
        long timestamp = in.readLong();
        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
        if (metadata == null) {
            String message = String.format("Got slice command for nonexistent table %s.%s.  If the table was just " + "created, this is likely due to the schema not being fully propagated.  Please wait for schema " + "agreement on table creation.", keyspaceName, cfName);
            throw new UnknownColumnFamilyException(message, null);
        }
        NamesQueryFilter filter = metadata.comparator.namesQueryFilterSerializer().deserialize(in, version);
        return new SliceByNamesReadCommand(keyspaceName, key, cfName, timestamp, filter).setIsDigestQuery(isDigest);
    }

    public long serializedSize(ReadCommand cmd, int version) {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceByNamesReadCommand command = (SliceByNamesReadCommand) cmd;
        int size = sizes.sizeof(command.isDigestQuery());
        int keySize = command.key.remaining();
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.cfName, "cmd.cfName").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.ksName, "cmd.ksName").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        CFMetaData metadata = Schema.instance.getCFMetaData(cmd.ksName, cmd.cfName);
        size += sizes.sizeof(command.ksName);
        size += sizes.sizeof((short) keySize) + keySize;
        size += sizes.sizeof(command.cfName);
        size += sizes.sizeof(cmd.timestamp);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(command, command.filter, "command.filter").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        size += metadata.comparator.namesQueryFilterSerializer().serializedSize(command.filter, version);
        return size;
    }
}
