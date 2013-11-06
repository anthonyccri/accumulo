package org.apache.accumulo.core.client.mapreduce;

import java.util.NoSuchElementException;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InputFormatBaseTest {
  
  private Configuration conf;
  
  @Before
  public void setup() {
    conf = new Configuration();
  }

  @Test
  public void testDefaultSequence() {
    AccumuloInputFormat.setInputInfo(conf, "root", "password".getBytes(), "table", new Authorizations("foo"));
    
    Assert.assertEquals(0, InputFormatBase.nextSequenceToProcess(conf));
    Assert.assertEquals(-1, InputFormatBase.nextSequenceToProcess(conf));
  }

  @Test
  public void testDefaultSequenceInputAndConnection() {
    AccumuloInputFormat.setInputInfo(conf, "root", "password".getBytes(), "table", new Authorizations("foo"));
    AccumuloInputFormat.setZooKeeperInstance(conf, "instance1", "zk1");
    
    Assert.assertEquals(0, InputFormatBase.nextSequenceToProcess(conf));
    Assert.assertEquals(-1, InputFormatBase.nextSequenceToProcess(conf));
  }

  @Test
  public void testDefaultWithCustomSequence() {
    AccumuloInputFormat.setInputInfo(conf, "root", "password".getBytes(), "table", new Authorizations("foo"));
    AccumuloInputFormat.setZooKeeperInstance(conf, "instance", "zk");
    
    int seq = AccumuloInputFormat.nextSequence(conf);
    
    Assert.assertEquals(1, seq);
    
    AccumuloInputFormat.setInputInfo(conf, seq, "root1", "password1".getBytes(), "table1", new Authorizations("foo1"));
    AccumuloInputFormat.setZooKeeperInstance(conf, seq, "instance1", "zk1");
    
    Assert.assertEquals(0, InputFormatBase.nextSequenceToProcess(conf));
    Assert.assertEquals(1, InputFormatBase.nextSequenceToProcess(conf));
    Assert.assertEquals(-1, InputFormatBase.nextSequenceToProcess(conf));
  }
  
  @Test
  public void testMultipleSequences() {
    int seq = AccumuloInputFormat.nextSequence(conf);
    
    AccumuloInputFormat.setInputInfo(conf,  seq, "root1", "password1".getBytes(), "table1", new Authorizations("foo1"));
    AccumuloInputFormat.setZooKeeperInstance(conf, seq, "instance1", "zk1");
    
    seq = AccumuloInputFormat.nextSequence(conf);
    
    AccumuloInputFormat.setInputInfo(conf, seq, "root2", "password2".getBytes(), "table2", new Authorizations("foo2"));
    AccumuloInputFormat.setZooKeeperInstance(conf, seq, "instance2", "zk2");
    
    seq = AccumuloInputFormat.nextSequence(conf);
    
    AccumuloInputFormat.setInputInfo(conf, seq, "root3", "password3".getBytes(), "table3", new Authorizations("foo3"));
    AccumuloInputFormat.setZooKeeperInstance(conf, seq, "instance3", "zk3");
    
    Assert.assertEquals(1, InputFormatBase.nextSequenceToProcess(conf));
    Assert.assertEquals(2, InputFormatBase.nextSequenceToProcess(conf));
    Assert.assertEquals(3, InputFormatBase.nextSequenceToProcess(conf));
    Assert.assertEquals(-1, InputFormatBase.nextSequenceToProcess(conf));
  }
  
  @Test(expected = NoSuchElementException.class)
  public void testNoSequences() {
    // When nothing was set, we should error
    InputFormatBase.nextSequenceToProcess(conf);
  }
}
