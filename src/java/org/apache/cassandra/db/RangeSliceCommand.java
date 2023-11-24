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
import java.util.ArrayList;
import java.util.List;
import com.google.common.base.Objects;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.pager.Pageable;

public class RangeSliceCommand extends AbstractRangeCommand implements Pageable {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final RangeSliceCommandSerializer serializer = new RangeSliceCommandSerializer();

    public final int maxResults;

    public final boolean countCQL3Rows;

    public final boolean isPaging;

    public RangeSliceCommand(String keyspace, String columnFamily, long timestamp, IDiskAtomFilter predicate, AbstractBounds<RowPosition> range, int maxResults) {
        this(keyspace, columnFamily, timestamp, predicate, range, null, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, String columnFamily, long timestamp, IDiskAtomFilter predicate, AbstractBounds<RowPosition> range, List<IndexExpression> row_filter, int maxResults) {
        this(keyspace, columnFamily, timestamp, predicate, range, row_filter, maxResults, false, false);
    }

    public RangeSliceCommand(String keyspace, String columnFamily, long timestamp, IDiskAtomFilter predicate, AbstractBounds<RowPosition> range, List<IndexExpression> rowFilter, int maxResults, boolean countCQL3Rows, boolean isPaging) {
        super(keyspace, columnFamily, timestamp, range, predicate, rowFilter);
        this.maxResults = maxResults;
        this.countCQL3Rows = countCQL3Rows;
        this.isPaging = isPaging;
    }

    public MessageOut<RangeSliceCommand> createMessage() {
        return new MessageOut<>(MessagingService.Verb.RANGE_SLICE, this, serializer);
    }

    public AbstractRangeCommand forSubRange(AbstractBounds<RowPosition> subRange) {
        return new RangeSliceCommand(keyspace, columnFamily, timestamp, predicate.cloneShallow(), subRange, rowFilter, maxResults, countCQL3Rows, isPaging);
    }

    public AbstractRangeCommand withUpdatedLimit(int newLimit) {
        return new RangeSliceCommand(keyspace, columnFamily, timestamp, predicate.cloneShallow(), keyRange, rowFilter, newLimit, countCQL3Rows, isPaging);
    }

    public int limit() {
        return maxResults;
    }

    public boolean countCQL3Rows() {
        return countCQL3Rows;
    }

    public List<Row> executeLocally() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.keyspace, "this.keyspace").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);
        ExtendedFilter exFilter = cfs.makeExtendedFilter(keyRange, predicate, rowFilter, maxResults, countCQL3Rows, isPaging, timestamp);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.rowFilter, "this.rowFilter").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        if (cfs.indexManager.hasIndexFor(rowFilter))
            return cfs.search(exFilter);
        else
            return cfs.getRangeSlice(exFilter);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("keyspace", keyspace).add("columnFamily", columnFamily).add("predicate", predicate).add("keyRange", keyRange).add("rowFilter", rowFilter).add("maxResults", maxResults).add("counterCQL3Rows", countCQL3Rows).add("timestamp", timestamp).toString();
    }
}

class RangeSliceCommandSerializer implements IVersionedSerializer<RangeSliceCommand> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public void serialize(RangeSliceCommand sliceCommand, DataOutputPlus out, int version) throws IOException {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.keyspace, "sliceCommand.keyspace").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(sliceCommand.keyspace);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.columnFamily, "sliceCommand.columnFamily").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeUTF(sliceCommand.columnFamily);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.timestamp, "sliceCommand.timestamp").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeLong(sliceCommand.timestamp);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.keyspace, "sliceCommand.keyspace").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.columnFamily, "sliceCommand.columnFamily").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        CFMetaData metadata = Schema.instance.getCFMetaData(sliceCommand.keyspace, sliceCommand.columnFamily);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.predicate, "sliceCommand.predicate").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        metadata.comparator.diskAtomFilterSerializer().serialize(sliceCommand.predicate, out, version);
        if (sliceCommand.rowFilter == null) {
            out.writeInt(0);
        } else {
            out.writeInt(sliceCommand.rowFilter.size());
            for (IndexExpression expr : sliceCommand.rowFilter) {
                expr.writeTo(out);
            }
        }
        MessagingService.validatePartitioner(sliceCommand.keyRange);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.keyRange, "sliceCommand.keyRange").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        AbstractBounds.rowPositionSerializer.serialize(sliceCommand.keyRange, out, version);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.maxResults, "sliceCommand.maxResults").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeInt(sliceCommand.maxResults);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.countCQL3Rows, "sliceCommand.countCQL3Rows").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeBoolean(sliceCommand.countCQL3Rows);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(sliceCommand, sliceCommand.isPaging, "sliceCommand.isPaging").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeBoolean(sliceCommand.isPaging);
    }

    public RangeSliceCommand deserialize(DataInput in, int version) throws IOException {
        String keyspace = in.readUTF();
        String columnFamily = in.readUTF();
        long timestamp = in.readLong();
        CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
        if (metadata == null) {
            String message = String.format("Got range slice command for nonexistent table %s.%s.  If the table was just " + "created, this is likely due to the schema not being fully propagated.  Please wait for schema " + "agreement on table creation.", keyspace, columnFamily);
            throw new UnknownColumnFamilyException(message, null);
        }
        IDiskAtomFilter predicate = metadata.comparator.diskAtomFilterSerializer().deserialize(in, version);
        List<IndexExpression> rowFilter;
        int filterCount = in.readInt();
        rowFilter = new ArrayList<>(filterCount);
        for (int i = 0; i < filterCount; i++) {
            rowFilter.add(IndexExpression.readFrom(in));
        }
        AbstractBounds<RowPosition> range = AbstractBounds.rowPositionSerializer.deserialize(in, MessagingService.globalPartitioner(), version);
        int maxResults = in.readInt();
        boolean countCQL3Rows = in.readBoolean();
        boolean isPaging = in.readBoolean();
        return new RangeSliceCommand(keyspace, columnFamily, timestamp, predicate, range, rowFilter, maxResults, countCQL3Rows, isPaging);
    }

    public long serializedSize(RangeSliceCommand rsc, int version) {
        long size = TypeSizes.NATIVE.sizeof(rsc.keyspace);
        size += TypeSizes.NATIVE.sizeof(rsc.columnFamily);
        size += TypeSizes.NATIVE.sizeof(rsc.timestamp);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(rsc, rsc.keyspace, "rsc.keyspace").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(rsc, rsc.columnFamily, "rsc.columnFamily").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        CFMetaData metadata = Schema.instance.getCFMetaData(rsc.keyspace, rsc.columnFamily);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(rsc, rsc.predicate, "rsc.predicate").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        IDiskAtomFilter filter = rsc.predicate;
        size += metadata.comparator.diskAtomFilterSerializer().serializedSize(filter, version);
        if (rsc.rowFilter == null) {
            size += TypeSizes.NATIVE.sizeof(0);
        } else {
            size += TypeSizes.NATIVE.sizeof(rsc.rowFilter.size());
            for (IndexExpression expr : rsc.rowFilter) {
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
                size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            }
        }
        size += AbstractBounds.rowPositionSerializer.serializedSize(rsc.keyRange, version);
        size += TypeSizes.NATIVE.sizeof(rsc.maxResults);
        size += TypeSizes.NATIVE.sizeof(rsc.countCQL3Rows);
        size += TypeSizes.NATIVE.sizeof(rsc.isPaging);
        return size;
    }
}
