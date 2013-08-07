/**
 * Copyright 2011, Deft Labs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.deftlabs.examples.mongo;

// Mongo
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DBAddress;
import com.mongodb.ServerAddress;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;

// JUnit
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

// Java
import java.util.List;
import java.util.ArrayList;

/**
 * An example of how to setup/use a replica set.
 */
public final class ReplicaSetMultipleSecondariesExample {

    private static final int [] _ports = { 27017, 27018, 27019, 27020, 27021 };
    private static final int [] _ports2 = { 27022, 27023, 27024, 27025 };

    @Before
    public void setupMongo() throws Exception {

        configureReplicaSet("aws1", _ports);

        // Sleep for a bit to wait for all the nodes to be intialized.
        Thread.sleep(20000);

        configureReplicaSet("aws2", _ports2);

        // Sleep for a bit to wait for all the nodes to be intialized.
        Thread.sleep(20000);
    }

        /**
     * Initialize a replica set for a shard.
     */
    private void configureReplicaSet(   final String pReplicaSetName,
                                        final int [] pPorts)
        throws Exception
    {
        // First we need to setup the replica sets.
        final BasicDBObject config = new BasicDBObject("_id", pReplicaSetName);

        final List<BasicDBObject> servers = new ArrayList<BasicDBObject>();

        int idx=0;
        for (final int port : pPorts) {
            final BasicDBObject server = new BasicDBObject("_id", idx++);
            server.put("host", ("localhost:" + port));

            if (idx == 2) server.put("arbiterOnly", true);

            servers.add(server);
        }

        config.put("members", servers);

        final Mongo mongo = new Mongo(new DBAddress("localhost", pPorts[0], "admin"));

        final CommandResult result
        = mongo.getDB("admin").command(new BasicDBObject("replSetInitiate", config));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void verifySetMembers() throws Exception {

        final Mongo mongo = new Mongo(new DBAddress("127.0.0.1", 27017, "admin"));

        final CommandResult result
        = mongo.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));

        final List<BasicDBObject> members = (List<BasicDBObject>)result.get("members");

        assertEquals(5, members.size());

        for (final BasicDBObject member : members) {
            //System.out.println(member);
        }
    }
}

