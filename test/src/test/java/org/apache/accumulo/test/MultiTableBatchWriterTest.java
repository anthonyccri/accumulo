/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;

public class MultiTableBatchWriterTest {
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static final String password = "secret";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), password);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  @Test
  public void testTableRenameSameWriters() throws Exception {
    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(password));

    BatchWriterConfig config = new BatchWriterConfig();

    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config, 60, TimeUnit.SECONDS);

    final String table1 = "testTableRenameSameWriters_table1", table2 = "testTableRenameSameWriters_table2";
    final String newTable1 = "testTableRenameSameWriters_newTable1", newTable2 = "testTableRenameSameWriters_newTable2";

    TableOperations tops = connector.tableOperations();
    tops.create(table1);
    tops.create(table2);

    BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

    Mutation m1 = new Mutation("foo");
    m1.put("col1", "", "val1");
    m1.put("col2", "", "val2");

    bw1.addMutation(m1);
    bw2.addMutation(m1);

    tops.rename(table1, newTable1);
    tops.rename(table2, newTable2);

    Mutation m2 = new Mutation("bar");
    m2.put("col1", "", "val1");
    m2.put("col2", "", "val2");

    bw1.addMutation(m2);
    bw2.addMutation(m2);

    mtbw.close();

    Map<Entry<String,String>,String> expectations = new HashMap<Entry<String,String>,String>();
    expectations.put(Maps.immutableEntry("foo", "col1"), "val1");
    expectations.put(Maps.immutableEntry("foo", "col2"), "val2");
    expectations.put(Maps.immutableEntry("bar", "col1"), "val1");
    expectations.put(Maps.immutableEntry("bar", "col2"), "val2");

    for (String table : Arrays.asList(newTable1, newTable2)) {
      Scanner s = connector.createScanner(table, new Authorizations());
      s.setRange(new Range());
      Map<Entry<String,String>,String> actual = new HashMap<Entry<String,String>,String>();
      for (Entry<Key,Value> entry : s) {
        actual.put(Maps.immutableEntry(entry.getKey().getRow().toString(), entry.getKey().getColumnFamily().toString()), entry.getValue().toString());
      }

      Assert.assertEquals("Differing results for " + table, expectations, actual);
    }
  }

  @Test
  public void testTableRenameNewWriters() throws Exception {
    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(password));

    BatchWriterConfig config = new BatchWriterConfig();

    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config, 60, TimeUnit.MINUTES);

    final String table1 = "testTableRenameNewWriters_table1", table2 = "testTableRenameNewWriters_table2";
    final String newTable1 = "testTableRenameNewWriters_newTable1", newTable2 = "testTableRenameNewWriters_newTable2";

    TableOperations tops = connector.tableOperations();
    tops.create(table1);
    tops.create(table2);

    BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

    Mutation m1 = new Mutation("foo");
    m1.put("col1", "", "val1");
    m1.put("col2", "", "val2");

    bw1.addMutation(m1);
    bw2.addMutation(m1);

    tops.rename(table1, newTable1);
    tops.rename(table2, newTable2);

    // MTBW is still caching this name to the correct table
    bw1 = mtbw.getBatchWriter(table1);
    bw2 = mtbw.getBatchWriter(table2);

    Mutation m2 = new Mutation("bar");
    m2.put("col1", "", "val1");
    m2.put("col2", "", "val2");

    bw1.addMutation(m2);
    bw2.addMutation(m2);

    mtbw.close();

    Map<Entry<String,String>,String> expectations = new HashMap<Entry<String,String>,String>();
    expectations.put(Maps.immutableEntry("foo", "col1"), "val1");
    expectations.put(Maps.immutableEntry("foo", "col2"), "val2");
    expectations.put(Maps.immutableEntry("bar", "col1"), "val1");
    expectations.put(Maps.immutableEntry("bar", "col2"), "val2");

    for (String table : Arrays.asList(newTable1, newTable2)) {
      Scanner s = connector.createScanner(table, new Authorizations());
      s.setRange(new Range());
      Map<Entry<String,String>,String> actual = new HashMap<Entry<String,String>,String>();
      for (Entry<Key,Value> entry : s) {
        actual.put(Maps.immutableEntry(entry.getKey().getRow().toString(), entry.getKey().getColumnFamily().toString()), entry.getValue().toString());
      }

      Assert.assertEquals("Differing results for " + table, expectations, actual);
    }
  }

  @Test
  public void testTableRenameNewWritersNoCaching() throws Exception {
    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(password));

    BatchWriterConfig config = new BatchWriterConfig();

    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config, 0, TimeUnit.SECONDS);

    final String table1 = "testTableRenameNewWritersNoCaching_table1", table2 = "testTableRenameNewWritersNoCaching_table2";
    final String newTable1 = "testTableRenameNewWritersNoCaching_newTable1", newTable2 = "testTableRenameNewWritersNoCaching_newTable2";

    TableOperations tops = connector.tableOperations();
    tops.create(table1);
    tops.create(table2);

    BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

    Mutation m1 = new Mutation("foo");
    m1.put("col1", "", "val1");
    m1.put("col2", "", "val2");

    bw1.addMutation(m1);
    bw2.addMutation(m1);

    tops.rename(table1, newTable1);
    tops.rename(table2, newTable2);

    try {
      bw1 = mtbw.getBatchWriter(table1);
      Assert.fail("Should not have gotten batchwriter for " + table1);
    } catch (TableNotFoundException e) {
      // Pass
    }

    try {
      bw2 = mtbw.getBatchWriter(table2);
    } catch (TableNotFoundException e) {
      // Pass
    }
  }

  @Test
  public void testTableDelete() throws Exception {
    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(password));

    BatchWriterConfig config = new BatchWriterConfig();

    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config, 60, TimeUnit.MINUTES);

    final String table1 = "testTableDelete_table1", table2 = "testTableDelete_table2";

    TableOperations tops = connector.tableOperations();
    tops.create(table1);
    tops.create(table2);

    BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

    Mutation m1 = new Mutation("foo");
    m1.put("col1", "", "val1");
    m1.put("col2", "", "val2");

    bw1.addMutation(m1);
    bw2.addMutation(m1);

    tops.delete(table1);
    tops.delete(table2);

    Mutation m2 = new Mutation("bar");
    m2.put("col1", "", "val1");
    m2.put("col2", "", "val2");

    bw1.addMutation(m2);
    bw2.addMutation(m2);

    try {
      mtbw.close();
      Assert.fail("Should not be able to close batch writers");
    } catch (MutationsRejectedException e) {
      // Pass
    }
  }

  @Test
  public void testOfflineTable() throws Exception {

    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(password));

    BatchWriterConfig config = new BatchWriterConfig();

    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config, 60, TimeUnit.MINUTES);

    final String table1 = "testOfflineTable_table1", table2 = "testOfflineTable_table2";

    TableOperations tops = connector.tableOperations();
    tops.create(table1);
    tops.create(table2);

    BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

    Mutation m1 = new Mutation("foo");
    m1.put("col1", "", "val1");
    m1.put("col2", "", "val2");

    bw1.addMutation(m1);
    bw2.addMutation(m1);

    tops.offline(table1);
    tops.offline(table2);

    Mutation m2 = new Mutation("bar");
    m2.put("col1", "", "val1");
    m2.put("col2", "", "val2");

    bw1.addMutation(m2);
    bw2.addMutation(m2);

    try {
      mtbw.close();
      Assert.fail("Should not be able to close batch writers");
    } catch (MutationsRejectedException e) {
      // Pass
    }
  }

  @Test
  public void testOfflineTableWithCache() throws Exception {

    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(password));

    BatchWriterConfig config = new BatchWriterConfig();

    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config, 60, TimeUnit.MINUTES);

    final String table1 = "testOfflineTableWithCache_table1", table2 = "testOfflineTableWithCache_table2";

    TableOperations tops = connector.tableOperations();
    tops.create(table1);
    tops.create(table2);

    BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

    Mutation m1 = new Mutation("foo");
    m1.put("col1", "", "val1");
    m1.put("col2", "", "val2");

    bw1.addMutation(m1);
    bw2.addMutation(m1);

    tops.offline(table1);
    tops.offline(table2);

    bw1 = mtbw.getBatchWriter(table1);
    bw2 = mtbw.getBatchWriter(table2);

    Mutation m2 = new Mutation("bar");
    m2.put("col1", "", "val1");
    m2.put("col2", "", "val2");

    bw1.addMutation(m2);
    bw2.addMutation(m2);

    try {
      mtbw.close();
      Assert.fail("Should not be able to close batch writers");
    } catch (MutationsRejectedException e) {
      // Pass
    }
  }

  @Test
  public void testOfflineTableWithoutCache() throws Exception {

    ZooKeeperInstance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector connector = instance.getConnector("root", new PasswordToken(password));

    BatchWriterConfig config = new BatchWriterConfig();

    MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(config, 0, TimeUnit.MINUTES);

    final String table1 = "testOfflineTableWithoutCache_table1", table2 = "testOfflineTableWithoutCache_table2";

    TableOperations tops = connector.tableOperations();
    tops.create(table1);
    tops.create(table2);

    BatchWriter bw1 = mtbw.getBatchWriter(table1), bw2 = mtbw.getBatchWriter(table2);

    Mutation m1 = new Mutation("foo");
    m1.put("col1", "", "val1");
    m1.put("col2", "", "val2");

    bw1.addMutation(m1);
    bw2.addMutation(m1);

    tops.offline(table1);
    tops.offline(table2);

    try {
      bw1 = mtbw.getBatchWriter(table1);
      Assert.fail(table1 + " should be offline");
    } catch (UncheckedExecutionException e) {
      Assert.assertEquals(TableOfflineException.class, e.getCause().getClass());
    }

    try {
      bw2 = mtbw.getBatchWriter(table2);
      Assert.fail(table1 + " should be offline");
    } catch (UncheckedExecutionException e) {
      Assert.assertEquals(TableOfflineException.class, e.getCause().getClass());
    }
  }

}
