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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.StreamingHistogram;

/**
 * SSTable metadata that always stay on heap.
 */
public class StatsMetadata extends MetadataComponent {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final IMetadataComponentSerializer serializer = new StatsMetadataSerializer();

    public final EstimatedHistogram estimatedRowSize;

    public final EstimatedHistogram estimatedColumnCount;

    public final ReplayPosition commitLogLowerBound;

    public final ReplayPosition commitLogUpperBound;

    public final long minTimestamp;

    public final long maxTimestamp;

    public final int maxLocalDeletionTime;

    public final double compressionRatio;

    public final StreamingHistogram estimatedTombstoneDropTime;

    public final int sstableLevel;

    public final List<ByteBuffer> maxColumnNames;

    public final List<ByteBuffer> minColumnNames;

    public final boolean hasLegacyCounterShards;

    public final long repairedAt;

    public StatsMetadata(EstimatedHistogram estimatedRowSize, EstimatedHistogram estimatedColumnCount, ReplayPosition commitLogLowerBound, ReplayPosition commitLogUpperBound, long minTimestamp, long maxTimestamp, int maxLocalDeletionTime, double compressionRatio, StreamingHistogram estimatedTombstoneDropTime, int sstableLevel, List<ByteBuffer> minColumnNames, List<ByteBuffer> maxColumnNames, boolean hasLegacyCounterShards, long repairedAt) {
        this.estimatedRowSize = estimatedRowSize;
        this.estimatedColumnCount = estimatedColumnCount;
        this.commitLogLowerBound = commitLogLowerBound;
        this.commitLogUpperBound = commitLogUpperBound;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.maxLocalDeletionTime = maxLocalDeletionTime;
        this.compressionRatio = compressionRatio;
        this.estimatedTombstoneDropTime = estimatedTombstoneDropTime;
        this.sstableLevel = sstableLevel;
        this.minColumnNames = minColumnNames;
        this.maxColumnNames = maxColumnNames;
        this.hasLegacyCounterShards = hasLegacyCounterShards;
        this.repairedAt = repairedAt;
    }

    public MetadataType getType() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.io.sstable.metadata.MetadataType.class, org.apache.cassandra.io.sstable.metadata.MetadataType.STATS, "org.apache.cassandra.io.sstable.metadata.MetadataType.STATS").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return MetadataType.STATS;
    }

    /**
     * @param gcBefore gc time in seconds
     * @return estimated droppable tombstone ratio at given gcBefore time.
     */
    public double getEstimatedDroppableTombstoneRatio(int gcBefore) {
        long estimatedColumnCount = this.estimatedColumnCount.mean() * this.estimatedColumnCount.count();
        if (estimatedColumnCount > 0) {
            double droppable = getDroppableTombstonesBefore(gcBefore);
            return droppable / estimatedColumnCount;
        }
        return 0.0f;
    }

    /**
     * @param gcBefore gc time in seconds
     * @return amount of droppable tombstones
     */
    public double getDroppableTombstonesBefore(int gcBefore) {
        return estimatedTombstoneDropTime.sum(gcBefore);
    }

    public StatsMetadata mutateLevel(int newLevel) {
        return new StatsMetadata(estimatedRowSize, estimatedColumnCount, commitLogLowerBound, commitLogUpperBound, minTimestamp, maxTimestamp, maxLocalDeletionTime, compressionRatio, estimatedTombstoneDropTime, newLevel, minColumnNames, maxColumnNames, hasLegacyCounterShards, repairedAt);
    }

    public StatsMetadata mutateRepairedAt(long newRepairedAt) {
        return new StatsMetadata(estimatedRowSize, estimatedColumnCount, commitLogLowerBound, commitLogUpperBound, minTimestamp, maxTimestamp, maxLocalDeletionTime, compressionRatio, estimatedTombstoneDropTime, sstableLevel, minColumnNames, maxColumnNames, hasLegacyCounterShards, newRepairedAt);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        StatsMetadata that = (StatsMetadata) o;
        return new EqualsBuilder().append(estimatedRowSize, that.estimatedRowSize).append(estimatedColumnCount, that.estimatedColumnCount).append(commitLogLowerBound, that.commitLogLowerBound).append(commitLogUpperBound, that.commitLogUpperBound).append(minTimestamp, that.minTimestamp).append(maxTimestamp, that.maxTimestamp).append(maxLocalDeletionTime, that.maxLocalDeletionTime).append(compressionRatio, that.compressionRatio).append(estimatedTombstoneDropTime, that.estimatedTombstoneDropTime).append(sstableLevel, that.sstableLevel).append(repairedAt, that.repairedAt).append(maxColumnNames, that.maxColumnNames).append(minColumnNames, that.minColumnNames).append(hasLegacyCounterShards, that.hasLegacyCounterShards).build();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(estimatedRowSize).append(estimatedColumnCount).append(commitLogLowerBound).append(commitLogUpperBound).append(minTimestamp).append(maxTimestamp).append(maxLocalDeletionTime).append(compressionRatio).append(estimatedTombstoneDropTime).append(sstableLevel).append(repairedAt).append(maxColumnNames).append(minColumnNames).append(hasLegacyCounterShards).build();
    }

    public static class StatsMetadataSerializer implements IMetadataComponentSerializer<StatsMetadata> {

        private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

        public int serializedSize(StatsMetadata component, Version version) throws IOException {
            int size = 0;
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedRowSize, TypeSizes.NATIVE);
            size += EstimatedHistogram.serializer.serializedSize(component.estimatedColumnCount, TypeSizes.NATIVE);
            size += ReplayPosition.serializer.serializedSize(component.commitLogUpperBound, TypeSizes.NATIVE);
            // mix/max timestamp(long), maxLocalDeletionTime(int), compressionRatio(double), repairedAt (long)
            size += 8 + 8 + 4 + 8 + 8;
            size += StreamingHistogram.serializer.serializedSize(component.estimatedTombstoneDropTime, TypeSizes.NATIVE);
            size += TypeSizes.NATIVE.sizeof(component.sstableLevel);
            // min column names
            size += 4;
            for (ByteBuffer columnName : component.minColumnNames) // with short length
            size += 2 + columnName.remaining();
            // max column names
            size += 4;
            for (ByteBuffer columnName : component.maxColumnNames) // with short length
            size += 2 + columnName.remaining();
            size += TypeSizes.NATIVE.sizeof(component.hasLegacyCounterShards);
            if (version.hasCommitLogLowerBound())
                size += ReplayPosition.serializer.serializedSize(component.commitLogLowerBound, TypeSizes.NATIVE);
            return size;
        }

        public void serialize(StatsMetadata component, Version version, DataOutputPlus out) throws IOException {
            EstimatedHistogram.serializer.serialize(component.estimatedRowSize, out);
            EstimatedHistogram.serializer.serialize(component.estimatedColumnCount, out);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.commitLogUpperBound, "component.commitLogUpperBound").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            ReplayPosition.serializer.serialize(component.commitLogUpperBound, out);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.minTimestamp, "component.minTimestamp").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeLong(component.minTimestamp);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.maxTimestamp, "component.maxTimestamp").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeLong(component.maxTimestamp);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.maxLocalDeletionTime, "component.maxLocalDeletionTime").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeInt(component.maxLocalDeletionTime);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.compressionRatio, "component.compressionRatio").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeDouble(component.compressionRatio);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.estimatedTombstoneDropTime, "component.estimatedTombstoneDropTime").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            StreamingHistogram.serializer.serialize(component.estimatedTombstoneDropTime, out);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.sstableLevel, "component.sstableLevel").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeInt(component.sstableLevel);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.repairedAt, "component.repairedAt").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeLong(component.repairedAt);
            out.writeInt(component.minColumnNames.size());
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.minColumnNames, "component.minColumnNames").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            for (ByteBuffer columnName : component.minColumnNames) {
                if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                    if (!isSerializeLoggingActive.get()) {
                        isSerializeLoggingActive.set(true);
                        serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component.minColumnNames, columnName, "columnName").toJsonString());
                        isSerializeLoggingActive.set(false);
                    }
                }
                ByteBufferUtil.writeWithShortLength(columnName, out);
            }
            out.writeInt(component.maxColumnNames.size());
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.maxColumnNames, "component.maxColumnNames").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            for (ByteBuffer columnName : component.maxColumnNames) {
                if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                    if (!isSerializeLoggingActive.get()) {
                        isSerializeLoggingActive.set(true);
                        serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component.maxColumnNames, columnName, "columnName").toJsonString());
                        isSerializeLoggingActive.set(false);
                    }
                }
                ByteBufferUtil.writeWithShortLength(columnName, out);
            }
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.hasLegacyCounterShards, "component.hasLegacyCounterShards").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeBoolean(component.hasLegacyCounterShards);
            if (version.hasCommitLogLowerBound()) {
                if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                    if (!isSerializeLoggingActive.get()) {
                        isSerializeLoggingActive.set(true);
                        serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.commitLogLowerBound, "component.commitLogLowerBound").toJsonString());
                        isSerializeLoggingActive.set(false);
                    }
                }
                ReplayPosition.serializer.serialize(component.commitLogLowerBound, out);
            }
        }

        public StatsMetadata deserialize(Version version, DataInput in) throws IOException {
            EstimatedHistogram rowSizes = EstimatedHistogram.serializer.deserialize(in);
            EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);
            ReplayPosition commitLogLowerBound = ReplayPosition.NONE, commitLogUpperBound;
            commitLogUpperBound = ReplayPosition.serializer.deserialize(in);
            long minTimestamp = in.readLong();
            long maxTimestamp = in.readLong();
            int maxLocalDeletionTime = in.readInt();
            double compressionRatio = in.readDouble();
            StreamingHistogram tombstoneHistogram = StreamingHistogram.serializer.deserialize(in);
            int sstableLevel = in.readInt();
            long repairedAt = 0;
            if (version.hasRepairedAt())
                repairedAt = in.readLong();
            int colCount = in.readInt();
            List<ByteBuffer> minColumnNames = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++) minColumnNames.add(ByteBufferUtil.readWithShortLength(in));
            colCount = in.readInt();
            List<ByteBuffer> maxColumnNames = new ArrayList<>(colCount);
            for (int i = 0; i < colCount; i++) maxColumnNames.add(ByteBufferUtil.readWithShortLength(in));
            boolean hasLegacyCounterShards = true;
            if (version.tracksLegacyCounterShards())
                hasLegacyCounterShards = in.readBoolean();
            if (version.hasCommitLogLowerBound())
                commitLogLowerBound = ReplayPosition.serializer.deserialize(in);
            return new StatsMetadata(rowSizes, columnCounts, commitLogLowerBound, commitLogUpperBound, minTimestamp, maxTimestamp, maxLocalDeletionTime, compressionRatio, tombstoneHistogram, sstableLevel, minColumnNames, maxColumnNames, hasLegacyCounterShards, repairedAt);
        }
    }
}
