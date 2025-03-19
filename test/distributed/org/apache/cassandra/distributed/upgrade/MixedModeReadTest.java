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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.distributed.test.ReadDigestConsistencyTest;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.distributed.test.ReadDigestConsistencyTest.CREATE_TABLE;
import static org.apache.cassandra.distributed.test.ReadDigestConsistencyTest.insertData;
import static org.apache.cassandra.distributed.test.ReadDigestConsistencyTest.testDigestConsistency;

public class MixedModeReadTest extends UpgradeTestBase
{
    private final static Logger logger = LoggerFactory.getLogger(MixedModeReadTest.class);

    @Test
    public void mixedModeReadColumnSubsetDigestCheck() throws Throwable
    {
        logger.info("[HKLOG] run a test 1");
        new TestCase()
        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
        .nodes(2)
        .nodesToUpgrade(1)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        // .upgrade(Versions.Major.v3X, Versions.Major.v4)
        .setup(cluster -> {
            cluster.schemaChange(CREATE_TABLE);
            insertData(cluster.coordinator(1));

            // print cluster java class
            logger.info("[HKLOG] cluster type = " + cluster.getClass().getName());
            logger.info("[HKLOG] v = " + cluster.get(1).getClass().getName());
            // getReleaseVersionString()
            logger.info("[HKLOG] n1 version = " + cluster.get(1).getReleaseVersionString());
            logger.info("[HKLOG] n2 version = " + cluster.get(2).getReleaseVersionString());
            logger.info("[HKLOG] cluster size = " + cluster.size());

            testDigestConsistency(cluster.coordinator(1));
            testDigestConsistency(cluster.coordinator(2));

            logger.info("[HKLOG] finish same version testing!");

        })
        .runAfterClusterUpgrade(cluster -> {
            // we need to let gossip settle or the test will fail
            int attempts = 1;
            //noinspection Convert2MethodRef
            while (!((IInvokableInstance) cluster.get(1)).callOnInstance(() -> Gossiper.instance.isUpgradingFromVersionLowerThan(CassandraVersion.CASSANDRA_4_0) &&
                                                                                 !Gossiper.instance.isUpgradingFromVersionLowerThan(new CassandraVersion(("3.0")).familyLowerBound.get())))
            {
                if (attempts++ > 90)
                    throw new RuntimeException("Gossiper.instance.haveMajorVersion3Nodes() continually returns false despite expecting to be true");
                Thread.sleep(1000);
            }

            // should not cause a disgest mismatch in mixed mode
            logger.info("[HKLOG] after upgrade n1 version = " + cluster.get(1).getReleaseVersionString());
            logger.info("[HKLOG] after upgrade n2 version = " + cluster.get(2).getReleaseVersionString());
            testDigestConsistency(cluster.coordinator(1));
            testDigestConsistency(cluster.coordinator(2));
        })
        .run();
    }
}
