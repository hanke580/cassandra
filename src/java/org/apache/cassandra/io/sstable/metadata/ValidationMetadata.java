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
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * SSTable metadata component used only for validating SSTable.
 *
 * This part is read before opening main Data.db file for validation
 * and discarded immediately after that.
 */
public class ValidationMetadata extends MetadataComponent {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final IMetadataComponentSerializer serializer = new ValidationMetadataSerializer();

    public final String partitioner;

    public final double bloomFilterFPChance;

    public ValidationMetadata(String partitioner, double bloomFilterFPChance) {
        this.partitioner = partitioner;
        this.bloomFilterFPChance = bloomFilterFPChance;
    }

    public MetadataType getType() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(org.apache.cassandra.io.sstable.metadata.MetadataType.class, org.apache.cassandra.io.sstable.metadata.MetadataType.VALIDATION, "org.apache.cassandra.io.sstable.metadata.MetadataType.VALIDATION").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return MetadataType.VALIDATION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ValidationMetadata that = (ValidationMetadata) o;
        return Double.compare(that.bloomFilterFPChance, bloomFilterFPChance) == 0 && partitioner.equals(that.partitioner);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = partitioner.hashCode();
        temp = Double.doubleToLongBits(bloomFilterFPChance);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    public static class ValidationMetadataSerializer implements IMetadataComponentSerializer<ValidationMetadata> {

        private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

        public int serializedSize(ValidationMetadata component, Version version) throws IOException {
            return TypeSizes.NATIVE.sizeof(component.partitioner) + 8;
        }

        public void serialize(ValidationMetadata component, Version version, DataOutputPlus out) throws IOException {
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.partitioner, "component.partitioner").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeUTF(component.partitioner);
            if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
                if (!isSerializeLoggingActive.get()) {
                    isSerializeLoggingActive.set(true);
                    serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(component, component.bloomFilterFPChance, "component.bloomFilterFPChance").toJsonString());
                    isSerializeLoggingActive.set(false);
                }
            }
            out.writeDouble(component.bloomFilterFPChance);
        }

        public ValidationMetadata deserialize(Version version, DataInput in) throws IOException {
            return new ValidationMetadata(in.readUTF(), in.readDouble());
        }
    }
}
