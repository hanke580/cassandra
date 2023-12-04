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
package org.apache.cassandra.serializers;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.json.simple.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class ObjectSerializer {
    private static final Logger logger = LoggerFactory.getLogger(ObjectSerializer.class);
    private static Random rand = new Random();

    public ObjectSerializer() {
    }

    static long elapsedTime = 0;
    public static void serialize(Object obj) {

        Long start = System.currentTimeMillis();
        try {
            File file = null;
            for (int i = 0; i < 65536; ++i) {
                int rn = rand.nextInt(2147483647);
                file = new File("/var/log/cassandra/memtable" +
                        Integer.toString(rn) +
                        ".ser");
                if (!file.exists()) {
                    break;
                }
            }
            FileWriter writer = new FileWriter(file);
            // logger.debug("serialize " + obj.getClass().toString());
            // logger.debug("is null " + (obj == null));
            // logger.debug("value " + (obj));
            JSONObject jsonObject = serializeRecursion(
                    obj, 0, new HashSet<String>());
            writer.write(jsonObject.toJSONString());
            writer.close();
        } catch (IOException e) {
        }
        Long end = System.currentTimeMillis();
        elapsedTime += end - start;
        logger.debug("elapsedTime " + elapsedTime);
    }

    public static JSONObject serializeRecursion(Object obj, int depth,
            Set<String> visitSet) {
        if (obj == null) {
            logger.debug("return null!");
            return null;
        }
        JSONObject struct = new JSONObject();
        {
            String hash = null;

            try {
                hash = obj.getClass().getName();
                hash += ";@" + obj.hashCode();
            } catch (Exception e) {
                logger.debug(
                        Arrays.toString(e.getStackTrace()));
            }
            struct.put("signature_hash", hash);
            if (visitSet.contains(hash) && !isConstant(obj)) {
                struct.put("type", "Virtual Object");
                return struct;
            } else {
                visitSet.add(hash);
            }

            // TODO skip enum
            // if (obj instanceof Object)
            // {
            // logger.debug(hash + " is a reference");
            // }
            // else
            // {
            // logger.debug(hash + " is not a reference");
            // }

            if (hash.startsWith("org.apache")) {
                try {
                    struct.put("type", "Object");
                    JSONObject subStruct = new JSONObject();
                    Field[] fields = obj.getClass()
                            .getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);
                        Object value = field.get(obj);
                        if (value == null) {
                            continue;
                        }
                        subStruct = serializeRecursion(
                                value, depth + 1,
                                visitSet);
                        struct.put(
                                "field::" +
                                        field.getName(),
                                subStruct);
                    }
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    logger.debug(e.getMessage(), e);
                }
            } else if (obj instanceof Iterable) {
                JSONArray subStruct = new JSONArray();
                Iterator<Object> iterator = ((Iterable<Object>) obj).iterator();
                struct.put("type", "Iterable");
                while (iterator.hasNext()) {
                    Object element = iterator.next();
                    JSONObject elementStruct = serializeRecursion(element,
                            depth + 1,
                            visitSet);
                    subStruct.add(elementStruct);
                }
                struct.put("Iterable", subStruct);
            } else if (obj instanceof Map) {
                struct.put("type", "Map");
                JSONArray subStruct = new JSONArray();
                int index = 0;
                for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) obj).entrySet()) {
                    JSONObject elementStruct = new JSONObject();
                    String keyStruct = entry.getKey().toString();
                    // JSONObject keyStruct = serializeRecursion(
                    //         entry.getKey(),
                    //         depth + 1, visitSet);
                    JSONObject valueStruct = serializeRecursion(
                            entry.getValue(),
                            depth + 1, visitSet);
                    elementStruct.put("key", keyStruct);
                    elementStruct.put("value", valueStruct);
                    subStruct.add(elementStruct);
                }
                struct.put("Map", subStruct);
            } else { // primitives
                struct.put("type", "Primitive");
                struct.put("value", obj.toString());
            }
        }
        return struct;
    }

    static private boolean isConstant(Object obj) {
        String signature = obj.getClass().getName();
        String[] whitelists = { "java.lang.Integer", "java.lang.Long",
                "java.lang.Boolean",
                "java.lang.String" };
        for (String whitelist : whitelists) {
            if (signature.startsWith(whitelist)) {
                return true;
            }
        }
        return false;
    }
}
