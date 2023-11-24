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
package org.apache.cassandra.gms;

import java.io.*;
import java.net.InetAddress;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;

/**
 * Contains information about a specified list of Endpoints and the largest version
 * of the state they have generated as known by the local endpoint.
 */
public class GossipDigest implements Comparable<GossipDigest> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final IVersionedSerializer<GossipDigest> serializer = new GossipDigestSerializer();

    final InetAddress endpoint;

    final int generation;

    final int maxVersion;

    GossipDigest(InetAddress ep, int gen, int version) {
        endpoint = ep;
        generation = gen;
        maxVersion = version;
    }

    InetAddress getEndpoint() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.endpoint, "this.endpoint").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return endpoint;
    }

    int getGeneration() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.generation, "this.generation").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return generation;
    }

    int getMaxVersion() {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.maxVersion, "this.maxVersion").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        return maxVersion;
    }

    public int compareTo(GossipDigest gDigest) {
        if (generation != gDigest.generation)
            return (generation - gDigest.generation);
        return (maxVersion - gDigest.maxVersion);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(endpoint);
        sb.append(":");
        sb.append(generation);
        sb.append(":");
        sb.append(maxVersion);
        return sb.toString();
    }
}

class GossipDigestSerializer implements IVersionedSerializer<GossipDigest> {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public void serialize(GossipDigest gDigest, DataOutputPlus out, int version) throws IOException {
        CompactEndpointSerializationHelper.serialize(gDigest.endpoint, out);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(gDigest, gDigest.generation, "gDigest.generation").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeInt(gDigest.generation);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(gDigest, gDigest.maxVersion, "gDigest.maxVersion").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        out.writeInt(gDigest.maxVersion);
    }

    public GossipDigest deserialize(DataInput in, int version) throws IOException {
        InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(in);
        int generation = in.readInt();
        int maxVersion = in.readInt();
        return new GossipDigest(endpoint, generation, maxVersion);
    }

    public long serializedSize(GossipDigest gDigest, int version) {
        long size = CompactEndpointSerializationHelper.serializedSize(gDigest.endpoint);
        size += TypeSizes.NATIVE.sizeof(gDigest.generation);
        size += TypeSizes.NATIVE.sizeof(gDigest.maxVersion);
        return size;
    }
}
