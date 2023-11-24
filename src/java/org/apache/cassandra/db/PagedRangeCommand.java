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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class PagedRangeCommand extends AbstractRangeCommand {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final IVersionedSerializer<PagedRangeCommand> serializer = new Serializer();

    public final Composite start;

    public final Composite stop;

    public final int limit;

    private final boolean countCQL3Rows;

    public PagedRangeCommand(String keyspace, String columnFamily, long timestamp, AbstractBounds<RowPosition> keyRange, SliceQueryFilter predicate, Composite start, Composite stop, List<IndexExpression> rowFilter, int limit, boolean countCQL3Rows) {
        super(keyspace, columnFamily, timestamp, keyRange, predicate, rowFilter);
        this.start = start;
        this.stop = stop;
        this.limit = limit;
        this.countCQL3Rows = countCQL3Rows;
    }

    public MessageOut<PagedRangeCommand> createMessage() {
        return new MessageOut<>(MessagingService.Verb.PAGED_RANGE, this, serializer);
    }

    public AbstractRangeCommand forSubRange(AbstractBounds<RowPosition> subRange) {
        Composite newStart = subRange.left.equals(keyRange.left) ? start : ((SliceQueryFilter) predicate).start();
        Composite newStop = subRange.right.equals(keyRange.right) ? stop : ((SliceQueryFilter) predicate).finish();
        return new PagedRangeCommand(keyspace, columnFamily, timestamp, subRange, ((SliceQueryFilter) predicate).cloneShallow(), newStart, newStop, rowFilter, limit, countCQL3Rows);
    }

    public AbstractRangeCommand withUpdatedLimit(int newLimit) {
        return new PagedRangeCommand(keyspace, columnFamily, timestamp, keyRange, ((SliceQueryFilter) predicate).cloneShallow(), start, stop, rowFilter, newLimit, countCQL3Rows);
    }

    public int limit() {
        return limit;
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
        ExtendedFilter exFilter = cfs.makeExtendedFilter(keyRange, (SliceQueryFilter) predicate, start, stop, rowFilter, limit, countCQL3Rows(), timestamp);
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
        return String.format("PagedRange(%s, %s, %d, %s, %s, %s, %s, %s, %d)", keyspace, columnFamily, timestamp, keyRange, predicate, start, stop, rowFilter, limit);
    }

    private static class Serializer implements IVersionedSerializer<PagedRangeCommand> {

        private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

        public void serialize(PagedRangeCommand cmd, DataOutputPlus out, int version) throws IOException {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.keyspace, "cmd.keyspace").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeUTF(cmd.keyspace);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.columnFamily, "cmd.columnFamily").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeUTF(cmd.columnFamily);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.timestamp, "cmd.timestamp").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeLong(cmd.timestamp);
            MessagingService.validatePartitioner(cmd.keyRange);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.keyRange, "cmd.keyRange").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            AbstractBounds.rowPositionSerializer.serialize(cmd.keyRange, out, version);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.columnFamily, "cmd.columnFamily").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.keyspace, "cmd.keyspace").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.predicate, "cmd.predicate").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            // SliceQueryFilter (the count is not used)
            SliceQueryFilter filter = (SliceQueryFilter) cmd.predicate;
            metadata.comparator.sliceQueryFilterSerializer().serialize(filter, out, version);
            // The start and stop of the page
            metadata.comparator.serializer().serialize(cmd.start, out);
            metadata.comparator.serializer().serialize(cmd.stop, out);
            out.writeInt(cmd.rowFilter.size());
            for (IndexExpression expr : cmd.rowFilter) {
                expr.writeTo(out);
                ;
            }
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.limit, "cmd.limit").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeInt(cmd.limit);
            if (version >= MessagingService.VERSION_21)
                out.writeBoolean(cmd.countCQL3Rows);
        }

        public PagedRangeCommand deserialize(DataInput in, int version) throws IOException {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            long timestamp = in.readLong();
            AbstractBounds<RowPosition> keyRange = AbstractBounds.rowPositionSerializer.deserialize(in, MessagingService.globalPartitioner(), version);
            CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
            if (metadata == null) {
                String message = String.format("Got paged range command for nonexistent table %s.%s.  If the table was just " + "created, this is likely due to the schema not being fully propagated.  Please wait for schema " + "agreement on table creation.", keyspace, columnFamily);
                throw new UnknownColumnFamilyException(message, null);
            }
            SliceQueryFilter predicate = metadata.comparator.sliceQueryFilterSerializer().deserialize(in, version);
            Composite start = metadata.comparator.serializer().deserialize(in);
            Composite stop = metadata.comparator.serializer().deserialize(in);
            int filterCount = in.readInt();
            List<IndexExpression> rowFilter = new ArrayList<IndexExpression>(filterCount);
            for (int i = 0; i < filterCount; i++) {
                rowFilter.add(IndexExpression.readFrom(in));
            }
            int limit = in.readInt();
            boolean countCQL3Rows = version >= MessagingService.VERSION_21 ? in.readBoolean() : // See #6857
            predicate.compositesToGroup >= 0 || predicate.count != 1;
            return new PagedRangeCommand(keyspace, columnFamily, timestamp, keyRange, predicate, start, stop, rowFilter, limit, countCQL3Rows);
        }

        public long serializedSize(PagedRangeCommand cmd, int version) {
            long size = 0;
            size += TypeSizes.NATIVE.sizeof(cmd.keyspace);
            size += TypeSizes.NATIVE.sizeof(cmd.columnFamily);
            size += TypeSizes.NATIVE.sizeof(cmd.timestamp);
            size += AbstractBounds.rowPositionSerializer.serializedSize(cmd.keyRange, version);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.keyspace, "cmd.keyspace").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.columnFamily, "cmd.columnFamily").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            CFMetaData metadata = Schema.instance.getCFMetaData(cmd.keyspace, cmd.columnFamily);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(cmd, cmd.predicate, "cmd.predicate").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            size += metadata.comparator.sliceQueryFilterSerializer().serializedSize((SliceQueryFilter) cmd.predicate, version);
            size += metadata.comparator.serializer().serializedSize(cmd.start, TypeSizes.NATIVE);
            size += metadata.comparator.serializer().serializedSize(cmd.stop, TypeSizes.NATIVE);
            size += TypeSizes.NATIVE.sizeof(cmd.rowFilter.size());
            for (IndexExpression expr : cmd.rowFilter) {
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.column);
                size += TypeSizes.NATIVE.sizeof(expr.operator.ordinal());
                size += TypeSizes.NATIVE.sizeofWithShortLength(expr.value);
            }
            size += TypeSizes.NATIVE.sizeof(cmd.limit);
            if (version >= MessagingService.VERSION_21)
                size += TypeSizes.NATIVE.sizeof(cmd.countCQL3Rows);
            return size;
        }
    }
}
