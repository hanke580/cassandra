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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Objects;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A range tombstone marker that represents a boundary between 2 range tombstones (i.e. it closes one range and open another).
 */
public class RangeTombstoneBoundaryMarker extends AbstractRangeTombstoneMarker {

    private final DeletionTime endDeletion;

    private final DeletionTime startDeletion;

    public RangeTombstoneBoundaryMarker(RangeTombstone.Bound bound, DeletionTime endDeletion, DeletionTime startDeletion) {
        super(bound);
        assert bound.isBoundary();
        this.endDeletion = endDeletion;
        this.startDeletion = startDeletion;
    }

    public static RangeTombstoneBoundaryMarker exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime closeDeletion, DeletionTime openDeletion) {
        RangeTombstone.Bound bound = RangeTombstone.Bound.exclusiveCloseInclusiveOpen(reversed, boundValues);
        DeletionTime endDeletion = reversed ? openDeletion : closeDeletion;
        DeletionTime startDeletion = reversed ? closeDeletion : openDeletion;
        return ((RangeTombstoneBoundaryMarker) org.zlab.ocov.tracker.Runtime.monitorCreationContext(new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion), 59));
    }

    public static RangeTombstoneBoundaryMarker inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime closeDeletion, DeletionTime openDeletion) {
        RangeTombstone.Bound bound = RangeTombstone.Bound.inclusiveCloseExclusiveOpen(reversed, boundValues);
        DeletionTime endDeletion = reversed ? openDeletion : closeDeletion;
        DeletionTime startDeletion = reversed ? closeDeletion : openDeletion;
        return ((RangeTombstoneBoundaryMarker) org.zlab.ocov.tracker.Runtime.monitorCreationContext(new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion), 60));
    }

    /**
     * The deletion time for the range tombstone this boundary ends (in clustering order).
     */
    public DeletionTime endDeletionTime() {
        return endDeletion;
    }

    /**
     * The deletion time for the range tombstone this boundary starts (in clustering order).
     */
    public DeletionTime startDeletionTime() {
        return startDeletion;
    }

    public DeletionTime closeDeletionTime(boolean reversed) {
        return reversed ? startDeletion : endDeletion;
    }

    public DeletionTime openDeletionTime(boolean reversed) {
        return reversed ? endDeletion : startDeletion;
    }

    public boolean openIsInclusive(boolean reversed) {
        return (bound.kind() == ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY) ^ reversed;
    }

    public RangeTombstone.Bound openBound(boolean reversed) {
        return bound.withNewKind(bound.kind().openBoundOfBoundary(reversed));
    }

    public RangeTombstone.Bound closeBound(boolean reversed) {
        return bound.withNewKind(bound.kind().closeBoundOfBoundary(reversed));
    }

    public boolean closeIsInclusive(boolean reversed) {
        return (bound.kind() == ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY) ^ reversed;
    }

    public boolean isOpen(boolean reversed) {
        // A boundary always open one side
        return true;
    }

    public boolean isClose(boolean reversed) {
        // A boundary always close one side
        return true;
    }

    public RangeTombstoneBoundaryMarker copy(AbstractAllocator allocator) {
        return ((RangeTombstoneBoundaryMarker) org.zlab.ocov.tracker.Runtime.monitorCreationContext(new RangeTombstoneBoundaryMarker(clustering().copy(allocator), endDeletion, startDeletion), 61));
    }

    public RangeTombstoneBoundaryMarker withNewOpeningDeletionTime(boolean reversed, DeletionTime newDeletionTime) {
        return ((RangeTombstoneBoundaryMarker) org.zlab.ocov.tracker.Runtime.monitorCreationContext(new RangeTombstoneBoundaryMarker(clustering(), reversed ? newDeletionTime : endDeletion, reversed ? startDeletion : newDeletionTime), 62));
    }

    public static RangeTombstoneBoundaryMarker makeBoundary(boolean reversed, Slice.Bound close, Slice.Bound open, DeletionTime closeDeletion, DeletionTime openDeletion) {
        assert RangeTombstone.Bound.Kind.compare(close.kind(), open.kind()) == 0 : "Both bound don't form a boundary";
        boolean isExclusiveClose = close.isExclusive() || (close.isInclusive() && open.isInclusive() && openDeletion.supersedes(closeDeletion));
        return isExclusiveClose ? exclusiveCloseInclusiveOpen(reversed, close.getRawValues(), closeDeletion, openDeletion) : inclusiveCloseExclusiveOpen(reversed, close.getRawValues(), closeDeletion, openDeletion);
    }

    public RangeTombstoneBoundMarker createCorrespondingCloseMarker(boolean reversed) {
        return ((RangeTombstoneBoundMarker) org.zlab.ocov.tracker.Runtime.monitorCreationContext(new RangeTombstoneBoundMarker(closeBound(reversed), endDeletion), 63));
    }

    public RangeTombstoneBoundMarker createCorrespondingOpenMarker(boolean reversed) {
        return ((RangeTombstoneBoundMarker) org.zlab.ocov.tracker.Runtime.monitorCreationContext(new RangeTombstoneBoundMarker(openBound(reversed), startDeletion), 64));
    }

    public void digest(MessageDigest digest) {
        bound.digest(digest);
        endDeletion.digest(digest);
        startDeletion.digest(digest);
    }

    public String toString(CFMetaData metadata) {
        return String.format("Marker %s@%d-%d", bound.toString(metadata), endDeletion.markedForDeleteAt(), startDeletion.markedForDeleteAt());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RangeTombstoneBoundaryMarker))
            return false;
        RangeTombstoneBoundaryMarker that = (RangeTombstoneBoundaryMarker) other;
        return this.bound.equals(that.bound) && this.endDeletion.equals(that.endDeletion) && this.startDeletion.equals(that.startDeletion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bound, endDeletion, startDeletion);
    }
}
