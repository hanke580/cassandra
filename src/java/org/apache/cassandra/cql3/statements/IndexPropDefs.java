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
package org.apache.cassandra.cql3.statements;

import java.util.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.exceptions.*;

public class IndexPropDefs extends PropertyDefinitions {

    private static final org.slf4j.Logger serialize_logger = org.slf4j.LoggerFactory.getLogger("serialize.logger");

    private java.lang.ThreadLocal<Boolean> isSerializeLoggingActive = new ThreadLocal<Boolean>() {

        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public static final String KW_OPTIONS = "options";

    public static final Set<String> keywords = new HashSet<>();

    public static final Set<String> obsoleteKeywords = new HashSet<>();

    public boolean isCustom;

    public String customClass;

    static {
        keywords.add(KW_OPTIONS);
    }

    public void validate() throws RequestValidationException {
        validate(keywords, obsoleteKeywords);
        if (isCustom && customClass == null)
            throw new InvalidRequestException("CUSTOM index requires specifiying the index class");
        if (!isCustom && customClass != null)
            throw new InvalidRequestException("Cannot specify index class for a non-CUSTOM index");
        if (!isCustom && !properties.isEmpty())
            throw new InvalidRequestException("Cannot specify options for a non-CUSTOM index");
        if (getRawOptions().containsKey(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME))
            throw new InvalidRequestException(String.format("Cannot specify %s as a CUSTOM option", SecondaryIndex.CUSTOM_INDEX_OPTION_NAME));
    }

    public Map<String, String> getRawOptions() throws SyntaxException {
        Map<String, String> options = getMap(KW_OPTIONS);
        return options == null ? Collections.<String, String>emptyMap() : options;
    }

    public Map<String, String> getOptions() throws SyntaxException {
        Map<String, String> options = new HashMap<>(getRawOptions());
        if (org.zlab.dinv.logger.SerializeMonitor.isSerializing) {
            if (!isSerializeLoggingActive.get()) {
                isSerializeLoggingActive.set(true);
                serialize_logger.info(org.zlab.dinv.logger.LogEntry.constructLogEntry(this, this.customClass, "this.customClass").toJsonString());
                isSerializeLoggingActive.set(false);
            }
        }
        options.put(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, customClass);
        return options;
    }
}
