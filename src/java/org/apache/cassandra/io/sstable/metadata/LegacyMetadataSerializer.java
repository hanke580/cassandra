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
package org.apache.cassandra.io.sstable.metadata;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.StreamingHistogram;

/**
 * Serializer for SSTable from legacy versions
 */
@Deprecated
public class LegacyMetadataSerializer extends MetadataSerializer {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    /**
     * Legacy serialization is only used for SSTable level reset.
     */
    @Override
    public void serialize(Map<MetadataType, MetadataComponent> components, Version version, DataOutputPlus out) throws IOException {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.io.sstable.metadata.MetadataType.class, org.apache.cassandra.io.sstable.metadata.MetadataType.VALIDATION, "org.apache.cassandra.io.sstable.metadata.MetadataType.VALIDATION").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        ValidationMetadata validation = (ValidationMetadata) components.get(MetadataType.VALIDATION);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.io.sstable.metadata.MetadataType.class, org.apache.cassandra.io.sstable.metadata.MetadataType.STATS, "org.apache.cassandra.io.sstable.metadata.MetadataType.STATS").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        StatsMetadata stats = (StatsMetadata) components.get(MetadataType.STATS);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.io.sstable.metadata.MetadataType.class, org.apache.cassandra.io.sstable.metadata.MetadataType.COMPACTION, "org.apache.cassandra.io.sstable.metadata.MetadataType.COMPACTION").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        CompactionMetadata compaction = (CompactionMetadata) components.get(MetadataType.COMPACTION);
        assert validation != null && stats != null && compaction != null && validation.partitioner != null;
        EstimatedHistogram.serializer.serialize(stats.estimatedRowSize, out);
        EstimatedHistogram.serializer.serialize(stats.estimatedColumnCount, out);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.commitLogUpperBound, "stats.commitLogUpperBound").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        ReplayPosition.serializer.serialize(stats.commitLogUpperBound, out);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.minTimestamp, "stats.minTimestamp").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeLong(stats.minTimestamp);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.maxTimestamp, "stats.maxTimestamp").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeLong(stats.maxTimestamp);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.maxLocalDeletionTime, "stats.maxLocalDeletionTime").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeInt(stats.maxLocalDeletionTime);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(validation, validation.bloomFilterFPChance, "validation.bloomFilterFPChance").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeDouble(validation.bloomFilterFPChance);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.compressionRatio, "stats.compressionRatio").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeDouble(stats.compressionRatio);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(validation, validation.partitioner, "validation.partitioner").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(validation.partitioner);
        out.writeInt(compaction.ancestors.size());
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(compaction, compaction.ancestors, "compaction.ancestors").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        for (Integer g : compaction.ancestors) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(compaction.ancestors, g, "g").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeInt(g);
        }
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.estimatedTombstoneDropTime, "stats.estimatedTombstoneDropTime").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        StreamingHistogram.serializer.serialize(stats.estimatedTombstoneDropTime, out);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.sstableLevel, "stats.sstableLevel").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeInt(stats.sstableLevel);
        out.writeInt(stats.minColumnNames.size());
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.minColumnNames, "stats.minColumnNames").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        for (ByteBuffer columnName : stats.minColumnNames) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats.minColumnNames, columnName, "columnName").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            ByteBufferUtil.writeWithShortLength(columnName, out);
        }
        out.writeInt(stats.maxColumnNames.size());
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.maxColumnNames, "stats.maxColumnNames").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        for (ByteBuffer columnName : stats.maxColumnNames) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats.maxColumnNames, columnName, "columnName").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            ByteBufferUtil.writeWithShortLength(columnName, out);
        }
        if (version.hasCommitLogLowerBound()) {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(stats, stats.commitLogLowerBound, "stats.commitLogLowerBound").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            ReplayPosition.serializer.serialize(stats.commitLogLowerBound, out);
        }
    }

    /**
     * Legacy serializer deserialize all components no matter what types are specified.
     */
    @Override
    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, EnumSet<MetadataType> types) throws IOException {
        Map<MetadataType, MetadataComponent> components = Maps.newHashMap();
        File statsFile = new File(descriptor.filenameFor(Component.STATS));
        if (!statsFile.exists() && types.contains(MetadataType.STATS)) {
            components.put(MetadataType.STATS, MetadataCollector.defaultStatsMetadata());
        } else {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(statsFile)))) {
                EstimatedHistogram rowSizes = EstimatedHistogram.serializer.deserialize(in);
                EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);
                ReplayPosition commitLogLowerBound = ReplayPosition.NONE;
                ReplayPosition commitLogUpperBound = ReplayPosition.serializer.deserialize(in);
                long minTimestamp = in.readLong();
                long maxTimestamp = in.readLong();
                int maxLocalDeletionTime = in.readInt();
                double bloomFilterFPChance = in.readDouble();
                double compressionRatio = in.readDouble();
                String partitioner = in.readUTF();
                int nbAncestors = in.readInt();
                Set<Integer> ancestors = new HashSet<>(nbAncestors);
                for (int i = 0; i < nbAncestors; i++) ancestors.add(in.readInt());
                StreamingHistogram tombstoneHistogram = StreamingHistogram.serializer.deserialize(in);
                int sstableLevel = 0;
                if (in.available() > 0)
                    sstableLevel = in.readInt();
                int colCount = in.readInt();
                List<ByteBuffer> minColumnNames = new ArrayList<>(colCount);
                for (int i = 0; i < colCount; i++) minColumnNames.add(ByteBufferUtil.readWithShortLength(in));
                colCount = in.readInt();
                List<ByteBuffer> maxColumnNames = new ArrayList<>(colCount);
                for (int i = 0; i < colCount; i++) maxColumnNames.add(ByteBufferUtil.readWithShortLength(in));
                if (descriptor.version.hasCommitLogLowerBound())
                    commitLogLowerBound = ReplayPosition.serializer.deserialize(in);
                if (types.contains(MetadataType.VALIDATION))
                    components.put(MetadataType.VALIDATION, new ValidationMetadata(partitioner, bloomFilterFPChance));
                if (types.contains(MetadataType.STATS))
                    components.put(MetadataType.STATS, new StatsMetadata(rowSizes, columnCounts, commitLogLowerBound, commitLogUpperBound, minTimestamp, maxTimestamp, maxLocalDeletionTime, compressionRatio, tombstoneHistogram, sstableLevel, minColumnNames, maxColumnNames, true, ActiveRepairService.UNREPAIRED_SSTABLE));
                if (types.contains(MetadataType.COMPACTION))
                    components.put(MetadataType.COMPACTION, new CompactionMetadata(ancestors, null));
            }
        }
        return components;
    }
}
