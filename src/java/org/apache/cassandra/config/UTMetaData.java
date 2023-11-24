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
package org.apache.cassandra.config;

import java.nio.ByteBuffer;
import java.util.*;
import org.apache.cassandra.db.marshal.*;

/**
 * Defined (and loaded) user types.
 *
 * In practice, because user types are global, we have only one instance of
 * this class that retrieve through the Schema class.
 */
public final class UTMetaData {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    private final Map<ByteBuffer, UserType> userTypes;

    public UTMetaData() {
        this(new HashMap<ByteBuffer, UserType>());
    }

    public UTMetaData(Map<ByteBuffer, UserType> types) {
        this.userTypes = types;
    }

    public UserType getType(ByteBuffer typeName) {
        return userTypes.get(typeName);
    }

    public Map<ByteBuffer, UserType> getAllTypes() {
        // Copy to avoid concurrent modification while iterating. Not intended to be called on a critical path anyway
        return new HashMap<>(userTypes);
    }

    // This is *not* thread safe but is only called in Schema that is synchronized.
    public void addType(UserType type) {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(type, type.name, "type.name").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        UserType old = userTypes.get(type.name);
        assert old == null || type.isCompatibleWith(old);
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(type, type.name, "type.name").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        userTypes.put(type.name, type);
    }

    // Same remarks than for addType
    public void removeType(UserType type) {
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(type, type.name, "type.name").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        userTypes.remove(type.name);
    }

    public boolean equals(Object that) {
        if (!(that instanceof UTMetaData))
            return false;
        return userTypes.equals(((UTMetaData) that).userTypes);
    }
}
