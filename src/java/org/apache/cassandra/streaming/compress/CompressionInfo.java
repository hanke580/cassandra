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
package org.apache.cassandra.streaming.compress;

import java.io.DataInput;
import java.io.IOException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Container that carries compression parameters and chunks to decompress data from stream.
 */
public class CompressionInfo {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    public static final IVersionedSerializer<CompressionInfo> serializer = new CompressionInfoSerializer();

    public final CompressionMetadata.Chunk[] chunks;

    public final CompressionParameters parameters;

    public CompressionInfo(CompressionMetadata.Chunk[] chunks, CompressionParameters parameters) {
        assert chunks != null && parameters != null;
        this.chunks = chunks;
        this.parameters = parameters;
    }

    static class CompressionInfoSerializer implements IVersionedSerializer<CompressionInfo> {

        private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

        public void serialize(CompressionInfo info, DataOutputPlus out, int version) throws IOException {
            if (info == null) {
                out.writeInt(-1);
                return;
            }
            int chunkCount = info.chunks.length;
            out.writeInt(chunkCount);
            for (int i = 0; i < chunkCount; i++) {
                if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                    if (!isSerializeLoggingActive.get()) {
                        isSerializeLoggingActive.set(true);
                        serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(info, info.chunks, "info.chunks").toJsonString());
                        isSerializeLoggingActive.set(false);
                    }
                }
                CompressionMetadata.Chunk.serializer.serialize(info.chunks[i], out, version);
                if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                    if (!isSerializeLoggingActive.get()) {
                        isSerializeLoggingActive.set(true);
                        serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(info.chunks, info.chunks[i], "info.chunks[i]").toJsonString());
                        isSerializeLoggingActive.set(false);
                    }
                }
            }
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(info, info.parameters, "info.parameters").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            // compression params
            CompressionParameters.serializer.serialize(info.parameters, out, version);
        }

        public CompressionInfo deserialize(DataInput in, int version) throws IOException {
            // chunks
            int chunkCount = in.readInt();
            if (chunkCount < 0)
                return null;
            CompressionMetadata.Chunk[] chunks = new CompressionMetadata.Chunk[chunkCount];
            for (int i = 0; i < chunkCount; i++) chunks[i] = CompressionMetadata.Chunk.serializer.deserialize(in, version);
            // compression params
            CompressionParameters parameters = CompressionParameters.serializer.deserialize(in, version);
            return new CompressionInfo(chunks, parameters);
        }

        public long serializedSize(CompressionInfo info, int version) {
            if (info == null)
                return TypeSizes.NATIVE.sizeof(-1);
            // chunks
            int chunkCount = info.chunks.length;
            long size = TypeSizes.NATIVE.sizeof(chunkCount);
            for (int i = 0; i < chunkCount; i++) {
                if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                    if (!isSerializeLoggingActive.get()) {
                        isSerializeLoggingActive.set(true);
                        serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(info, info.chunks, "info.chunks").toJsonString());
                        isSerializeLoggingActive.set(false);
                    }
                }
                size += CompressionMetadata.Chunk.serializer.serializedSize(info.chunks[i], version);
            }
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(info, info.parameters, "info.parameters").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            // compression params
            size += CompressionParameters.serializer.serializedSize(info.parameters, version);
            return size;
        }
    }
}
