/**
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
package org.apache.accumulo.core.client.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Test;

/**
 * 
 */
public class AccumuloOutputFormatTest {
  static class TestMapper extends Mapper<Key,Value,Text,Mutation> {
    Key key = null;
    int count = 0;
    
    @Override
    protected void map(Key k, Value v, Context context) throws IOException, InterruptedException {
      if (key != null)
        assertEquals(key.getRow().toString(), new String(v.get()));
      assertEquals(k.getRow(), new Text(String.format("%09x", count + 1)));
      assertEquals(new String(v.get()), String.format("%09x", count));
      key = new Key(k);
      count++;
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      Mutation m = new Mutation("total");
      m.put("", "", Integer.toString(count));
      try {
        context.write(new Text(), m);
      } catch (NullPointerException e) {}
    }
  }
  
  @Test
  public void testMR() throws Exception {
    MockInstance mockInstance = new MockInstance("testmrinstance");
    Connector c = mockInstance.getConnector("root", new byte[] {});
    c.tableOperations().create("testtable1");
    c.tableOperations().create("testtable2");
    BatchWriter bw = c.createBatchWriter("testtable1", 10000L, 1000L, 4);
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation(new Text(String.format("%09x", i + 1)));
      m.put(new Text(), new Text(), new Value(String.format("%09x", i).getBytes()));
      bw.addMutation(m);
    }
    bw.close();
    
    Job job = new Job();
    job.setInputFormatClass(AccumuloInputFormat.class);
    job.setMapperClass(TestMapper.class);
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Mutation.class);
    job.setNumReduceTasks(0);
    Configuration conf = job.getConfiguration();
    AccumuloInputFormat.setInputInfo(conf, "root", "".getBytes(), "testtable1", new Authorizations());
    AccumuloInputFormat.setMockInstance(conf, "testmrinstance");
    AccumuloOutputFormat.setOutputInfo(conf, "root", "".getBytes(), false, "testtable2");
    AccumuloOutputFormat.setMockInstance(conf, "testmrinstance");
    
    AccumuloInputFormat input = new AccumuloInputFormat();
    List<InputSplit> splits = input.getSplits(job);
    assertEquals(splits.size(), 1);
    
    AccumuloOutputFormat output = new AccumuloOutputFormat();
    
    TestMapper mapper = (TestMapper) job.getMapperClass().newInstance();
    for (InputSplit split : splits) {
      TaskAttemptID id = new TaskAttemptID();
      TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), id);
      RecordReader<Key,Value> reader = input.createRecordReader(split, tac);
      RecordWriter<Text,Mutation> writer = output.getRecordWriter(tac);
      Mapper<Key,Value,Text,Mutation>.Context context = mapper.new Context(job.getConfiguration(), id, reader, writer, null, null, split);
      reader.initialize(split, context);
      mapper.run(context);
      writer.close(context);
    }
    
    Scanner scanner = c.createScanner("testtable2", new Authorizations());
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    assertTrue(iter.hasNext());
    Entry<Key,Value> entry = iter.next();
    assertEquals(Integer.parseInt(new String(entry.getValue().get())), 100);
    assertFalse(iter.hasNext());
  }
  
  @Test
  public void testMultiInstanceConfiguration() throws Exception {
    int seq1 = AccumuloOutputFormat.nextSequence(), seq2 = AccumuloOutputFormat.nextSequence();
    
    Configuration conf = new Configuration();
    AccumuloOutputFormat.setOutputInfo(conf, seq1, "root1", "1".getBytes(), false, "testtable1");
    AccumuloOutputFormat.setMockInstance(conf, seq1, "testinstance1");
    
    AccumuloOutputFormat.setOutputInfo(conf, seq2, "root2", "2".getBytes(), true, "testtable2");
    AccumuloOutputFormat.setMockInstance(conf, seq2, "testinstance2");
    
    assertEquals("root1", AccumuloOutputFormat.getUsername(conf, seq1));
    assertEquals("1", new String(AccumuloOutputFormat.getPassword(conf, seq1)));
    assertEquals(false, AccumuloOutputFormat.canCreateTables(conf, seq1));
    assertEquals("testtable1", AccumuloOutputFormat.getDefaultTableName(conf, seq1));
    
    Instance inst1 = AccumuloOutputFormat.getInstance(conf, seq1);
    assertEquals("testinstance1", inst1.getInstanceName());
    
    assertEquals("root2", AccumuloOutputFormat.getUsername(conf, seq2));
    assertEquals("2", new String(AccumuloOutputFormat.getPassword(conf, seq2)));
    assertEquals(true, AccumuloOutputFormat.canCreateTables(conf, seq2));
    assertEquals("testtable2", AccumuloOutputFormat.getDefaultTableName(conf, seq2));
    
    Instance inst2 = AccumuloOutputFormat.getInstance(conf, seq2);
    assertEquals("testinstance2", inst2.getInstanceName());
  }
  
  @Test
  public void testConfigEntries() throws Exception {
    Configuration conf = new Configuration();
    int seq1 = AccumuloOutputFormat.nextSequence(), seq2 = AccumuloOutputFormat.nextSequence();
    
    AccumuloOutputFormat.setOutputInfo(conf, seq1, "root1", "1".getBytes(), false, "testtable1");
    AccumuloOutputFormat.setZooKeeperInstance(conf, seq1, "instance1", "zk1");
    
    AccumuloOutputFormat.setOutputInfo(conf, seq2, "root2", "2".getBytes(), true, "testtable2");
    AccumuloOutputFormat.setZooKeeperInstance(conf, seq2, "instance2", "zk2");
    
    final String prefix = AccumuloOutputFormat.class.getSimpleName();
    HashMap<String,String> expected = new HashMap<String,String>();
    expected.put(prefix + ".username.1", "root1");
    expected.put(prefix + ".password.1", new String(Base64.encodeBase64("1".getBytes())));
    expected.put(prefix + ".createtables.1", "false");
    expected.put(prefix + ".defaulttable.1", "testtable1");
    expected.put(prefix + ".instanceName.1", "instance1");
    expected.put(prefix + ".zooKeepers.1", "zk1");
    expected.put(prefix + ".configured.1", "true");
    expected.put(prefix + ".instanceConfigured.1", "true");

    expected.put(prefix + ".username.2", "root2");
    expected.put(prefix + ".password.2", new String(Base64.encodeBase64("2".getBytes())));
    expected.put(prefix + ".createtables.2", "true");
    expected.put(prefix + ".defaulttable.2", "testtable2");
    expected.put(prefix + ".instanceName.2", "instance2");
    expected.put(prefix + ".zooKeepers.2", "zk2");
    expected.put(prefix + ".configured.2", "true");
    expected.put(prefix + ".instanceConfigured.2", "true");
    
    Map<String,String> actual = AccumuloOutputFormat.getRelevantEntries(conf);
    
    assertEquals(expected, actual);
  }
}
