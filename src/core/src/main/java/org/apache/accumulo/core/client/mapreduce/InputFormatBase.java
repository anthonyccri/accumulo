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
package org.apache.accumulo.core.client.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;

import javax.servlet.jsp.jstl.core.Config;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.mock.MockTabletLocator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * This class allows MapReduce jobs to use Accumulo as the source of data. This input format provides keys and values of type K and V to the Map() and Reduce()
 * functions.
 * 
 * Subclasses must implement the following method: public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws
 * IOException, InterruptedException
 * 
 * This class includes a static class that can be used to create a RecordReader: protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V>
 * 
 * Subclasses of RecordReaderBase must implement the following method: public boolean nextKeyValue() throws IOException, InterruptedException This method should
 * set the following variables: K currentK V currentV Key currentKey (used for progress reporting) int numKeysRead (used for progress reporting)
 * 
 * See AccumuloInputFormat for an example implementation.
 * 
 * Other static methods are optional
 */

public abstract class InputFormatBase<K,V> extends InputFormat<K,V> {
  protected static final Logger log = Logger.getLogger(InputFormatBase.class);

  private static final String PREFIX = AccumuloInputFormat.class.getSimpleName();
  private static final String INPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
  private static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
  private static final String USERNAME = PREFIX + ".username";
  private static final String PASSWORD = PREFIX + ".password";
  private static final String TABLE_NAME = PREFIX + ".tablename";
  private static final String AUTHORIZATIONS = PREFIX + ".authorizations";

  private static final String INSTANCE_NAME = PREFIX + ".instanceName";
  private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
  private static final String MOCK = ".useMockInstance";

  private static final String RANGES = PREFIX + ".ranges";
  private static final String AUTO_ADJUST_RANGES = PREFIX + ".ranges.autoAdjust";

  private static final String ROW_REGEX = PREFIX + ".regex.row";
  private static final String COLUMN_FAMILY_REGEX = PREFIX + ".regex.cf";
  private static final String COLUMN_QUALIFIER_REGEX = PREFIX + ".regex.cq";
  private static final String VALUE_REGEX = PREFIX + ".regex.value";

  private static final String COLUMNS = PREFIX + ".columns";
  private static final String LOGLEVEL = PREFIX + ".loglevel";

  private static final String ISOLATED = PREFIX + ".isolated";

  private static final String LOCAL_ITERATORS = PREFIX + ".localiters";

  // Used to specify the maximum # of versions of an Accumulo cell value to return
  private static final String MAX_VERSIONS = PREFIX + ".maxVersions";

  // Used for specifying the iterators to be applied
  private static final String ITERATORS = PREFIX + ".iterators";
  private static final String ITERATORS_OPTIONS = PREFIX + ".iterators.options";
  private static final String ITERATORS_DELIM = ",";

  private static final String SEQ_DELIM = ".";
  protected static final int DEFAULT_SEQUENCE = 0;
  
  private static final String COMMA = ",";
  private static final String CONFIGURED_SEQUENCES = PREFIX + ".configuredsSeqs";
  private static final String DEFAULT_SEQ_USED = PREFIX + ".defaultSequenceUsed";
  private static final String PROCESSED_SEQUENCES = PREFIX + ".processedSeqs";
  private static final String TRUE = "true";

  private static final String READ_OFFLINE = PREFIX + ".read.offline";

  /**
   * Get a unique identifier for these configurations
   * 
   * @return A unique number to provide to future AccumuloInputFormat calls
   */
  public static synchronized int nextSequence(Configuration conf) {
    String value = conf.get(CONFIGURED_SEQUENCES);
    if (null == value) {
      conf.set(CONFIGURED_SEQUENCES, "1");
      return 1;
    } else {
      String[] splitValues = StringUtils.split(value, COMMA);
      int newValue = Integer.parseInt(splitValues[splitValues.length-1]) + 1;
      
      conf.set(CONFIGURED_SEQUENCES, value + COMMA + newValue);
      return newValue;
    }
  }
  
  /**
   * Using the provided Configuration, return the next sequence number to process.
   * @param conf A Configuration object used to store AccumuloInputFormat information into
   * @return The next sequence number to process, -1 when finished.
   * @throws NoSuchElementException
   */
  protected static synchronized int nextSequenceToProcess(Configuration conf) throws NoSuchElementException {
    String[] processedConfs = conf.getStrings(PROCESSED_SEQUENCES);
    
    // We haven't set anything, so we need to find the first to return
    if (null == processedConfs || 0 == processedConfs.length) {
      // Check to see if the default sequence was used
      boolean defaultSeqUsed = conf.getBoolean(DEFAULT_SEQ_USED, false);
      
      // If so, set that we're processing it and return the value of the default
      if (defaultSeqUsed) {
        conf.set(PROCESSED_SEQUENCES, Integer.toString(DEFAULT_SEQUENCE));
        return DEFAULT_SEQUENCE;
      }
      
      String[] loadedConfs = conf.getStrings(CONFIGURED_SEQUENCES);
      
      // There was *nothing* loaded, fail.
      if (null == loadedConfs || 0 == loadedConfs.length) {
        throw new NoSuchElementException("Sequence was requested to process but none exist to return");
      }
      
      // We have loaded configuration(s), use the first
      int firstLoaded = Integer.parseInt(loadedConfs[0]);
      conf.setInt(PROCESSED_SEQUENCES, firstLoaded);
      
      return firstLoaded;
    }
    
    // We've previously parsed some confs, need to find the next one to load
    int lastProcessedSeq = Integer.valueOf(processedConfs[processedConfs.length - 1]);
    String[] configuredSequencesArray = conf.getStrings(CONFIGURED_SEQUENCES);
    
    // We only have the default sequence, no specifics.
    // Getting here, we already know that we processed that default
    if (null == configuredSequencesArray) {
      return -1;
    }

    List<Integer> configuredSequences = new ArrayList<Integer>(configuredSequencesArray.length + 1);
    
    // If we used the default sequence ID, add that into the list of configured sequences
    if (conf.getBoolean(DEFAULT_SEQ_USED, false)) {
      configuredSequences.add(DEFAULT_SEQUENCE);
    }

    // Add the rest of any sequences to our list
    for (String configuredSequence : configuredSequencesArray) {
      configuredSequences.add(Integer.parseInt(configuredSequence));
    }
    
    int lastParsedSeqIndex = configuredSequences.size() - 1;
    
    // Find the next sequence number after the one we last processed
    for (; lastParsedSeqIndex >= 0; lastParsedSeqIndex--) {
      int lastLoadedValue = configuredSequences.get(lastParsedSeqIndex);
      
      if (lastLoadedValue == lastProcessedSeq) {
        break;
      }
    }
    
    // We either had no sequences to match or we matched the last configured sequence
    // Both of which are equivalent to no (more) sequences to process
    if (-1 == lastParsedSeqIndex || lastParsedSeqIndex + 1 >= configuredSequences.size()) {
      return -1;
    }
    
    // Get the value of the sequence at that offset
    int nextSequence = configuredSequences.get(lastParsedSeqIndex + 1);
    conf.set(PROCESSED_SEQUENCES, conf.get(PROCESSED_SEQUENCES) + COMMA + nextSequence);
    
    return nextSequence;
  }
  
  protected static void setDefaultSequenceUsed(Configuration conf) {
    String value = conf.get(DEFAULT_SEQ_USED);
    if (null == value || !TRUE.equals(value)) {
      conf.setBoolean(DEFAULT_SEQ_USED, true);
    }
  }

  protected static String merge(String name, Integer sequence) {
    return name + SEQ_DELIM + sequence;
  }
  
  public static Map<String,String> getRelevantEntries(Configuration conf) {
    ArgumentChecker.notNull(conf);
    
    HashMap<String,String> confEntries = new HashMap<String,String>();
    for (Entry<String,String> entry : conf) {
      final String key = entry.getKey();
      if (0 == key.indexOf(PREFIX)) {
        confEntries.put(key, entry.getValue());
      }
    }
    
    return confEntries;
  }

  /**
   * @deprecated Use {@link #setIsolated(Configuration,boolean)} instead
   */
  public static void setIsolated(JobContext job, boolean enable) {
    setIsolated(job.getConfiguration(), enable);
  }

  /**
   * Enable or disable use of the {@link IsolatedScanner} in this configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the IsolatedScanner. Otherwise, disable.
   */
  public static void setIsolated(Configuration conf, boolean enable) {
    setDefaultSequenceUsed(conf);
    setIsolated(conf, DEFAULT_SEQUENCE, enable);
  }

  /**
   * Enable or disable use of the {@link IsolatedScanner} in this configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the IsolatedScanner. Otherwise, disable.
   */
  public static void setIsolated(Configuration conf, int sequence, boolean enable) {
    conf.setBoolean(merge(ISOLATED, sequence), enable);
  }

  /**
   * @deprecated Use {@link #setLocalIterators(Configuration,boolean)} instead
   */
  public static void setLocalIterators(JobContext job, boolean enable) {
    setLocalIterators(job.getConfiguration(), enable);
  }

  /**
   * Enable or disable use of the {@link ClientSideIteratorScanner} in this Configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the ClientSideInteratorScanner. Otherwise, disable.
   */
  public static void setLocalIterators(Configuration conf, boolean enable) {
    setDefaultSequenceUsed(conf);
    setLocalIterators(conf, DEFAULT_SEQUENCE, enable);
  }

  /**
   * Enable or disable use of the {@link ClientSideIteratorScanner} in this Configuration object. By default it is not enabled.
   * 
   * @param conf
   *          The Hadoop configuration object
   * @param enable
   *          if true, enable usage of the ClientSideInteratorScanner. Otherwise, disable.
   */
  public static void setLocalIterators(Configuration conf, int sequence, boolean enable) {
    conf.setBoolean(merge(LOCAL_ITERATORS, sequence), enable);
  }

  /**
   * @deprecated Use {@link #setInputInfo(Configuration,String,byte[],String,Authorizations)} instead
   */
  public static void setInputInfo(JobContext job, String user, byte[] passwd, String table, Authorizations auths) {
    setInputInfo(job.getConfiguration(), user, passwd, table, auths);
  }

  /**
   * Initialize the user, table, and authorization information for the configuration object that will be used with an Accumulo InputFormat.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param user
   *          a valid accumulo user
   * @param passwd
   *          the user's password
   * @param table
   *          the table to read
   * @param auths
   *          the authorizations used to restrict data read
   */
  public static void setInputInfo(Configuration conf, String user, byte[] passwd, String table, Authorizations auths) {
    setDefaultSequenceUsed(conf);
    setInputInfo(conf, DEFAULT_SEQUENCE, user, passwd, table, auths);
  }

  /**
   * Initialize the user, table, and authorization information for the configuration object that will be used with an Accumulo InputFormat.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param user
   *          a valid accumulo user
   * @param passwd
   *          the user's password
   * @param table
   *          the table to read
   * @param auths
   *          the authorizations used to restrict data read
   */
  public static void setInputInfo(Configuration conf, int sequence, String user, byte[] passwd, String table, Authorizations auths) {
    final String inputInfoSet = merge(INPUT_INFO_HAS_BEEN_SET, sequence);
    if (conf.getBoolean(inputInfoSet, false))
      throw new IllegalStateException("Input info for sequence " + sequence + " can only be set once per job");
    conf.setBoolean(inputInfoSet, true);

    ArgumentChecker.notNull(user, passwd, table);
    conf.set(merge(USERNAME, sequence), user);
    conf.set(merge(PASSWORD, sequence), new String(Base64.encodeBase64(passwd)));
    conf.set(merge(TABLE_NAME, sequence), table);
    if (auths != null && !auths.isEmpty())
      conf.set(merge(AUTHORIZATIONS, sequence), auths.serialize());
  }

  /**
   * @deprecated Use {@link #setZooKeeperInstance(Configuration,String,String)} instead
   */
  public static void setZooKeeperInstance(JobContext job, String instanceName, String zooKeepers) {
    setZooKeeperInstance(job.getConfiguration(), instanceName, zooKeepers);
  }

  /**
   * Configure a {@link ZooKeeperInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   */
  public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
    setDefaultSequenceUsed(conf);
    setZooKeeperInstance(conf, DEFAULT_SEQUENCE, instanceName, zooKeepers);
  }

  /**
   * Configure a {@link ZooKeeperInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   * @param zooKeepers
   *          a comma-separated list of zookeeper servers
   */
  public static void setZooKeeperInstance(Configuration conf, int sequence, String instanceName, String zooKeepers) {
    final String instanceInfoSet = merge(INSTANCE_HAS_BEEN_SET, sequence);
    if (conf.getBoolean(instanceInfoSet, false))
      throw new IllegalStateException("Instance info for sequence " + sequence + " can only be set once per job");
    conf.setBoolean(instanceInfoSet, true);

    ArgumentChecker.notNull(instanceName, zooKeepers);
    conf.set(merge(INSTANCE_NAME, sequence), instanceName);
    conf.set(merge(ZOOKEEPERS, sequence), zooKeepers);
  }

  /**
   * @deprecated Use {@link #setMockInstance(Configuration,String)} instead
   */
  public static void setMockInstance(JobContext job, String instanceName) {
    setMockInstance(job.getConfiguration(), instanceName);
  }

  /**
   * Configure a {@link MockInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   */
  public static void setMockInstance(Configuration conf, String instanceName) {
    setDefaultSequenceUsed(conf);
    setMockInstance(conf, DEFAULT_SEQUENCE, instanceName);
  }

  /**
   * Configure a {@link MockInstance} for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param instanceName
   *          the accumulo instance name
   */
  public static void setMockInstance(Configuration conf, int sequence, String instanceName) {
    conf.setBoolean(merge(INSTANCE_HAS_BEEN_SET, sequence), true);
    conf.setBoolean(merge(MOCK, sequence), true);
    conf.set(merge(INSTANCE_NAME, sequence), instanceName);
  }

  /**
   * @deprecated Use {@link #setRanges(Configuration,Collection)} instead
   */
  public static void setRanges(JobContext job, Collection<Range> ranges) {
    setRanges(job.getConfiguration(), ranges);
  }

  /**
   * Set the ranges to map over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param ranges
   *          the ranges that will be mapped over
   */
  public static void setRanges(Configuration conf, Collection<Range> ranges) {
    setDefaultSequenceUsed(conf);
    setRanges(conf, DEFAULT_SEQUENCE, ranges);
  }

  /**
   * Set the ranges to map over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param ranges
   *          the ranges that will be mapped over
   */
  public static void setRanges(Configuration conf, int sequence, Collection<Range> ranges) {
    ArgumentChecker.notNull(ranges);
    ArrayList<String> rangeStrings = new ArrayList<String>(ranges.size());
    try {
      for (Range r : ranges) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        r.write(new DataOutputStream(baos));
        rangeStrings.add(new String(Base64.encodeBase64(baos.toByteArray())));
      }
    } catch (IOException ex) {
      throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
    }
    conf.setStrings(merge(RANGES, sequence), rangeStrings.toArray(new String[0]));
  }

  /**
   * @deprecated Use {@link #disableAutoAdjustRanges(Configuration)} instead
   */
  public static void disableAutoAdjustRanges(JobContext job) {
    disableAutoAdjustRanges(job.getConfiguration());
  }

  /**
   * Disables the adjustment of ranges for this configuration object. By default, overlapping ranges will be merged and ranges will be fit to existing tablet
   * boundaries. Disabling this adjustment will cause there to be exactly one mapper per range set using {@link #setRanges(Configuration, Collection)}.
   * 
   * @param conf
   *          the Hadoop configuration object
   */
  public static void disableAutoAdjustRanges(Configuration conf) {
    setDefaultSequenceUsed(conf);
    disableAutoAdjustRanges(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Disables the adjustment of ranges for this configuration object. By default, overlapping ranges will be merged and ranges will be fit to existing tablet
   * boundaries. Disabling this adjustment will cause there to be exactly one mapper per range set using {@link #setRanges(Configuration, Collection)}.
   * 
   * @param conf
   *          the Hadoop configuration object
   */
  public static void disableAutoAdjustRanges(Configuration conf, int sequence) {
    conf.setBoolean(merge(AUTO_ADJUST_RANGES, sequence), false);
  }

  /**
   * @deprecated since 1.4 use {@link org.apache.accumulo.core.iterators.user.RegExFilter} and {@link #addIterator(Configuration, IteratorSetting)}
   */
  public static enum RegexType {
    ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, VALUE
  }

  /**
   * @deprecated since 1.4 use {@link #addIterator(Configuration, IteratorSetting)}
   * @see org.apache.accumulo.core.iterators.user.RegExFilter#setRegexs(IteratorSetting, String, String, String, String, boolean)
   * @param job
   * @param type
   * @param regex
   */
  public static void setRegex(JobContext job, RegexType type, String regex) {
    setDefaultSequenceUsed(job.getConfiguration());
    setRegex(job, DEFAULT_SEQUENCE, type, regex);
  }

  /**
   * @deprecated since 1.4 use {@link #addIterator(Configuration, IteratorSetting)}
   * @see org.apache.accumulo.core.iterators.user.RegExFilter#setRegexs(IteratorSetting, String, String, String, String, boolean)
   * @param job
   * @param type
   * @param regex
   */
  public static void setRegex(JobContext job, int sequence, RegexType type, String regex) {
    ArgumentChecker.notNull(type, regex);
    String key = null;
    switch (type) {
      case ROW:
        key = ROW_REGEX;
        break;
      case COLUMN_FAMILY:
        key = COLUMN_FAMILY_REGEX;
        break;
      case COLUMN_QUALIFIER:
        key = COLUMN_QUALIFIER_REGEX;
        break;
      case VALUE:
        key = VALUE_REGEX;
        break;
      default:
        throw new NoSuchElementException();
    }
    try {
      job.getConfiguration().set(merge(key, sequence), URLEncoder.encode(regex, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      log.error("Failedd to encode regular expression", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @deprecated Use {@link #setMaxVersions(Configuration,int)} instead
   */
  public static void setMaxVersions(JobContext job, int maxVersions) throws IOException {
    setMaxVersions(job.getConfiguration(), maxVersions);
  }

  /**
   * Sets the max # of values that may be returned for an individual Accumulo cell. By default, applied before all other Accumulo iterators (highest priority)
   * leveraged in the scan by the record reader. To adjust priority use setIterator() & setIteratorOptions() w/ the VersioningIterator type explicitly.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param maxVersions
   *          the max number of versions per accumulo cell
   * @throws IOException
   *           if maxVersions is < 1
   */
  public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
    setDefaultSequenceUsed(conf);
    setMaxVersions(conf, DEFAULT_SEQUENCE, maxVersions);
  }

  /**
   * Sets the max # of values that may be returned for an individual Accumulo cell. By default, applied before all other Accumulo iterators (highest priority)
   * leveraged in the scan by the record reader. To adjust priority use setIterator() & setIteratorOptions() w/ the VersioningIterator type explicitly.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param maxVersions
   *          the max number of versions per accumulo cell
   * @throws IOException
   *           if maxVersions is < 1
   */
  public static void setMaxVersions(Configuration conf, int sequence, int maxVersions) throws IOException {
    if (maxVersions < 1)
      throw new IOException("Invalid maxVersions: " + maxVersions + ".  Must be >= 1");
    conf.setInt(merge(MAX_VERSIONS, sequence), maxVersions);
  }

  /**
   * <p>
   * Enable reading offline tables. This will make the map reduce job directly read the tables files. If the table is not offline, then the job will fail. If
   * the table comes online during the map reduce job, its likely that the job will fail.
   * 
   * <p>
   * To use this option, the map reduce user will need access to read the accumulo directory in HDFS.
   * 
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
   * on the mappers classpath. The accumulo-site.xml may need to be on the mappers classpath if HDFS or the accumlo directory in HDFS are non-standard.
   * 
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as the input table for a map reduce job. If you plan to map
   * reduce over the data many times, it may be better to the compact the table, clone it, take it offline, and use the clone for all map reduce jobs. The
   * reason to do this is that compaction will reduce each tablet in the table to one file, and its faster to read from one file.
   * 
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may see better read performance. Second, it will support
   * speculative execution better. When reading an online table speculative execution can put more load on an already slow tablet server.
   * 
   * @param conf
   *          the job
   * @param scanOff
   *          pass true to read offline tables
   */

  public static void setScanOffline(Configuration conf, boolean scanOff) {
    setDefaultSequenceUsed(conf);
    setScanOffline(conf, DEFAULT_SEQUENCE, scanOff);
  }

  /**
   * <p>
   * Enable reading offline tables. This will make the map reduce job directly read the tables files. If the table is not offline, then the job will fail. If
   * the table comes online during the map reduce job, its likely that the job will fail.
   * 
   * <p>
   * To use this option, the map reduce user will need access to read the accumulo directory in HDFS.
   * 
   * <p>
   * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
   * on the mappers classpath. The accumulo-site.xml may need to be on the mappers classpath if HDFS or the accumlo directory in HDFS are non-standard.
   * 
   * <p>
   * One way to use this feature is to clone a table, take the clone offline, and use the clone as the input table for a map reduce job. If you plan to map
   * reduce over the data many times, it may be better to the compact the table, clone it, take it offline, and use the clone for all map reduce jobs. The
   * reason to do this is that compaction will reduce each tablet in the table to one file, and its faster to read from one file.
   * 
   * <p>
   * There are two possible advantages to reading a tables file directly out of HDFS. First, you may see better read performance. Second, it will support
   * speculative execution better. When reading an online table speculative execution can put more load on an already slow tablet server.
   * 
   * @param conf
   *          the job
   * @param scanOff
   *          pass true to read offline tables
   */

  public static void setScanOffline(Configuration conf, int sequence, boolean scanOff) {
    conf.setBoolean(merge(READ_OFFLINE, sequence), scanOff);
  }

  /**
   * @deprecated Use {@link #fetchColumns(Configuration,Collection)} instead
   */
  public static void fetchColumns(JobContext job, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    fetchColumns(job.getConfiguration(), columnFamilyColumnQualifierPairs);
  }

  /**
   * Restricts the columns that will be mapped over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param columnFamilyColumnQualifierPairs
   *          A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   */
  public static void fetchColumns(Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    setDefaultSequenceUsed(conf);
    fetchColumns(conf, DEFAULT_SEQUENCE, columnFamilyColumnQualifierPairs);
  }

  /**
   * Restricts the columns that will be mapped over for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param columnFamilyColumnQualifierPairs
   *          A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family is
   *          selected. An empty set is the default and is equivalent to scanning the all columns.
   */
  public static void fetchColumns(Configuration conf, int sequence, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
    ArgumentChecker.notNull(columnFamilyColumnQualifierPairs);
    ArrayList<String> columnStrings = new ArrayList<String>(columnFamilyColumnQualifierPairs.size());
    for (Pair<Text,Text> column : columnFamilyColumnQualifierPairs) {
      if (column.getFirst() == null)
        throw new IllegalArgumentException("Column family can not be null for sequence " + sequence);

      String col = new String(Base64.encodeBase64(TextUtil.getBytes(column.getFirst())));
      if (column.getSecond() != null)
        col += ":" + new String(Base64.encodeBase64(TextUtil.getBytes(column.getSecond())));
      columnStrings.add(col);
    }
    conf.setStrings(merge(COLUMNS, sequence), columnStrings.toArray(new String[0]));
  }

  /**
   * @deprecated Use {@link #setLogLevel(Configuration,Level)} instead
   */
  public static void setLogLevel(JobContext job, Level level) {
    setLogLevel(job.getConfiguration(), level);
  }

  /**
   * Sets the log level for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param level
   *          the logging level
   */
  public static void setLogLevel(Configuration conf, Level level) {
    setDefaultSequenceUsed(conf);
    setLogLevel(conf, DEFAULT_SEQUENCE, level);
  }

  /**
   * Sets the log level for this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @param level
   *          the logging level
   */
  public static void setLogLevel(Configuration conf, int sequence, Level level) {
    ArgumentChecker.notNull(level);
    // TODO We can't differentiate logging levels with the sequence number approach
    log.setLevel(level);
    conf.setInt(merge(LOGLEVEL, sequence), level.toInt());
  }

  /**
   * @deprecated Use {@link #addIterator(Configuration,IteratorSetting)} instead
   */
  public static void addIterator(JobContext job, IteratorSetting cfg) {
    addIterator(job.getConfiguration(), cfg);
  }

  /**
   * Encode an iterator on the input for this configuration object.
   * 
   * @param conf
   *          The Hadoop configuration in which to save the iterator configuration
   * @param cfg
   *          The configuration of the iterator
   */
  public static void addIterator(Configuration conf, IteratorSetting cfg) {
    setDefaultSequenceUsed(conf);
    addIterator(conf, DEFAULT_SEQUENCE, cfg);
  }

  /**
   * Encode an iterator on the input for this configuration object.
   * 
   * @param conf
   *          The Hadoop configuration in which to save the iterator configuration
   * @param cfg
   *          The configuration of the iterator
   */
  public static void addIterator(Configuration conf, int sequence, IteratorSetting cfg) {
    // First check to see if anything has been set already
    String iterators = conf.get(merge(ITERATORS, sequence));

    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()).toString();
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()).toString());
    }
    // Store the iterators w/ the job
    conf.set(merge(ITERATORS, sequence), iterators);
    for (Entry<String,String> entry : cfg.getOptions().entrySet()) {
      if (entry.getValue() == null)
        continue;

      String iteratorOptions = conf.get(merge(ITERATORS_OPTIONS, sequence));

      // No options specified yet, create a new string
      if (iteratorOptions == null || iteratorOptions.isEmpty()) {
        iteratorOptions = new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()).toString();
      } else {
        // append the next option & reset
        iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()));
      }

      // Store the options w/ the job
      conf.set(merge(ITERATORS_OPTIONS, sequence), iteratorOptions);
    }
  }

  /**
   * Specify an Accumulo iterator type to manage the behavior of the underlying table scan this InputFormat's RecordReader will conduct, w/ priority dictating
   * the order in which specified iterators are applied. Repeat calls to specify multiple iterators are allowed.
   * 
   * @param job
   *          the job
   * @param priority
   *          the priority
   * @param iteratorClass
   *          the iterator class
   * @param iteratorName
   *          the iterator name
   * 
   * @deprecated since 1.4, see {@link #addIterator(Configuration, IteratorSetting)}
   */
  public static void setIterator(JobContext job, int priority, String iteratorClass, String iteratorName) {
    setDefaultSequenceUsed(job.getConfiguration());
    setIterator(job, DEFAULT_SEQUENCE, priority, iteratorClass, iteratorName);
  }

  /**
   * Specify an Accumulo iterator type to manage the behavior of the underlying table scan this InputFormat's RecordReader will conduct, w/ priority dictating
   * the order in which specified iterators are applied. Repeat calls to specify multiple iterators are allowed.
   * 
   * @param job
   *          the job
   * @param priority
   *          the priority
   * @param iteratorClass
   *          the iterator class
   * @param iteratorName
   *          the iterator name
   * 
   * @deprecated since 1.4, see {@link #addIterator(Configuration, IteratorSetting)}
   */
  public static void setIterator(JobContext job, int sequence, int priority, String iteratorClass, String iteratorName) {
    // First check to see if anything has been set already
    String iterators = job.getConfiguration().get(merge(ITERATORS, sequence));

    // No iterators specified yet, create a new string
    if (iterators == null || iterators.isEmpty()) {
      iterators = new AccumuloIterator(priority, iteratorClass, iteratorName).toString();
    } else {
      // append the next iterator & reset
      iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(priority, iteratorClass, iteratorName).toString());
    }
    // Store the iterators w/ the job
    job.getConfiguration().set(merge(ITERATORS, sequence), iterators);

  }

  /**
   * Specify an option for a named Accumulo iterator, further specifying that iterator's behavior.
   * 
   * @param job
   *          the job
   * @param iteratorName
   *          the iterator name. Should correspond to an iterator set w/ a prior setIterator call.
   * @param key
   *          the key
   * @param value
   *          the value
   * 
   * @deprecated since 1.4, see {@link #addIterator(Configuration, IteratorSetting)}
   */
  public static void setIteratorOption(JobContext job, String iteratorName, String key, String value) {
    setDefaultSequenceUsed(job.getConfiguration());
    setIteratorOption(job, DEFAULT_SEQUENCE, iteratorName, key, value);
  }

  /**
   * Specify an option for a named Accumulo iterator, further specifying that iterator's behavior.
   * 
   * @param job
   *          the job
   * @param iteratorName
   *          the iterator name. Should correspond to an iterator set w/ a prior setIterator call.
   * @param key
   *          the key
   * @param value
   *          the value
   * 
   * @deprecated since 1.4, see {@link #addIterator(Configuration, IteratorSetting)}
   */
  public static void setIteratorOption(JobContext job, int sequence, String iteratorName, String key, String value) {
    if (iteratorName == null || key == null || value == null)
      return;

    String iteratorOptions = job.getConfiguration().get(merge(ITERATORS_OPTIONS, sequence));

    // No options specified yet, create a new string
    if (iteratorOptions == null || iteratorOptions.isEmpty()) {
      iteratorOptions = new AccumuloIteratorOption(iteratorName, key, value).toString();
    } else {
      // append the next option & reset
      iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(iteratorName, key, value));
    }

    // Store the options w/ the job
    job.getConfiguration().set(merge(ITERATORS_OPTIONS, sequence), iteratorOptions);
  }

  /**
   * @deprecated Use {@link #isIsolated(Configuration)} instead
   */
  protected static boolean isIsolated(JobContext job) {
    return isIsolated(job.getConfiguration());
  }

  /**
   * Determines whether a configuration has isolation enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if isolation is enabled, false otherwise
   * @see #setIsolated(Configuration, boolean)
   */
  protected static boolean isIsolated(Configuration conf) {
    return isIsolated(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Determines whether a configuration has isolation enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if isolation is enabled, false otherwise
   * @see #setIsolated(Configuration, boolean)
   */
  protected static boolean isIsolated(Configuration conf, int sequence) {
    return conf.getBoolean(merge(ISOLATED, sequence), false);
  }

  /**
   * @deprecated Use {@link #usesLocalIterators(Configuration)} instead
   */
  protected static boolean usesLocalIterators(JobContext job) {
    return usesLocalIterators(job.getConfiguration());
  }

  /**
   * Determines whether a configuration uses local iterators.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if uses local iterators, false otherwise
   * @see #setLocalIterators(Configuration, boolean)
   */
  protected static boolean usesLocalIterators(Configuration conf) {
    return usesLocalIterators(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Determines whether a configuration uses local iterators.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if uses local iterators, false otherwise
   * @see #setLocalIterators(Configuration, boolean)
   */
  protected static boolean usesLocalIterators(Configuration conf, int sequence) {
    return conf.getBoolean(merge(LOCAL_ITERATORS, sequence), false);
  }

  /**
   * @deprecated Use {@link #getUsername(Configuration)} instead
   */
  protected static String getUsername(JobContext job) {
    return getUsername(job.getConfiguration());
  }

  /**
   * Gets the user name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the user name
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static String getUsername(Configuration conf) {
    return getUsername(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the user name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the user name
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static String getUsername(Configuration conf, int sequence) {
    return conf.get(merge(USERNAME, sequence));
  }

  /**
   * WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to provide a charset safe conversion to a
   * string, and is not intended to be secure.
   * 
   * @deprecated Use {@link #getPassword(Configuration)} instead
   */
  protected static byte[] getPassword(JobContext job) {
    return getPassword(job.getConfiguration());
  }

  /**
   * Gets the password from the configuration. WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to
   * provide a charset safe conversion to a string, and is not intended to be secure.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the BASE64-encoded password
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static byte[] getPassword(Configuration conf) {
    return getPassword(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the password from the configuration. WARNING: The password is stored in the Configuration and shared with all MapReduce tasks; It is BASE64 encoded to
   * provide a charset safe conversion to a string, and is not intended to be secure.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the BASE64-encoded password
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static byte[] getPassword(Configuration conf, int sequence) {
    return Base64.decodeBase64(conf.get(merge(PASSWORD, sequence), "").getBytes());
  }

  /**
   * @deprecated Use {@link #getTablename(Configuration)} instead
   */
  protected static String getTablename(JobContext job) {
    return getTablename(job.getConfiguration());
  }

  /**
   * Gets the table name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the table name
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static String getTablename(Configuration conf) {
    return getTablename(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the table name from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the table name
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static String getTablename(Configuration conf, int sequence) {
    return conf.get(merge(TABLE_NAME, sequence));
  }

  /**
   * @deprecated Use {@link #getAuthorizations(Configuration)} instead
   */
  protected static Authorizations getAuthorizations(JobContext job) {
    return getAuthorizations(job.getConfiguration());
  }

  /**
   * Gets the authorizations to set for the scans from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the accumulo scan authorizations
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static Authorizations getAuthorizations(Configuration conf) {
    return getAuthorizations(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the authorizations to set for the scans from the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the accumulo scan authorizations
   * @see #setInputInfo(Configuration, String, byte[], String, Authorizations)
   */
  protected static Authorizations getAuthorizations(Configuration conf, int sequence) {
    final String authString = conf.get(merge(AUTHORIZATIONS, sequence));
    return authString == null ? Constants.NO_AUTHS : new Authorizations(authString.split(","));
  }

  /**
   * @deprecated Use {@link #getInstance(Configuration)} instead
   */
  protected static Instance getInstance(JobContext job) {
    return getInstance(job.getConfiguration());
  }

  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo instance
   * @see #setZooKeeperInstance(Configuration, String, String)
   * @see #setMockInstance(Configuration, String)
   */
  protected static Instance getInstance(Configuration conf) {
    return getInstance(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Initializes an Accumulo {@link Instance} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo instance
   * @see #setZooKeeperInstance(Configuration, String, String)
   * @see #setMockInstance(Configuration, String)
   */
  protected static Instance getInstance(Configuration conf, int sequence) {
    if (conf.getBoolean(merge(MOCK, sequence), false))
      return new MockInstance(conf.get(merge(INSTANCE_NAME, sequence)));
    return new ZooKeeperInstance(conf.get(merge(INSTANCE_NAME, sequence)), conf.get(merge(ZOOKEEPERS, sequence)));
  }

  /**
   * @deprecated Use {@link #getTabletLocator(Configuration)} instead
   */
  protected static TabletLocator getTabletLocator(JobContext job) throws TableNotFoundException {
    return getTabletLocator(job.getConfiguration());
  }

  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   */
  protected static TabletLocator getTabletLocator(Configuration conf) throws TableNotFoundException {
    return getTabletLocator(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Initializes an Accumulo {@link TabletLocator} based on the configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return an accumulo tablet locator
   * @throws TableNotFoundException
   *           if the table name set on the configuration doesn't exist
   */
  protected static TabletLocator getTabletLocator(Configuration conf, int sequence) throws TableNotFoundException {
    if (conf.getBoolean(merge(MOCK, sequence), false))
      return new MockTabletLocator();
    Instance instance = getInstance(conf, sequence);
    String username = getUsername(conf, sequence);
    byte[] password = getPassword(conf, sequence);
    String tableName = getTablename(conf, sequence);
    return TabletLocator.getInstance(instance, new AuthInfo(username, ByteBuffer.wrap(password), instance.getInstanceID()),
        new Text(Tables.getTableId(instance, tableName)));
  }

  /**
   * @deprecated Use {@link #getRanges(Configuration)} instead
   */
  protected static List<Range> getRanges(JobContext job) throws IOException {
    return getRanges(job.getConfiguration());
  }

  /**
   * Gets the ranges to scan over from a configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @see #setRanges(Configuration, Collection)
   */
  protected static List<Range> getRanges(Configuration conf) throws IOException {
    return getRanges(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the ranges to scan over from a configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the ranges
   * @throws IOException
   *           if the ranges have been encoded improperly
   * @see #setRanges(Configuration, Collection)
   */
  protected static List<Range> getRanges(Configuration conf, int sequence) throws IOException {
    ArrayList<Range> ranges = new ArrayList<Range>();
    for (String rangeString : conf.getStringCollection(merge(RANGES, sequence))) {
      ByteArrayInputStream bais = new ByteArrayInputStream(Base64.decodeBase64(rangeString.getBytes()));
      Range range = new Range();
      range.readFields(new DataInputStream(bais));
      ranges.add(range);
    }
    return ranges;
  }

  /**
   * @deprecated since 1.4 use {@link org.apache.accumulo.core.iterators.user.RegExFilter} and {@link #addIterator(Configuration, IteratorSetting)}
   * @see #setRegex(JobContext, RegexType, String)
   */
  protected static String getRegex(JobContext job, RegexType type) {
    return getRegex(job, DEFAULT_SEQUENCE, type);
  }

  /**
   * @deprecated since 1.4 use {@link org.apache.accumulo.core.iterators.user.RegExFilter} and {@link #addIterator(Configuration, IteratorSetting)}
   * @see #setRegex(JobContext, RegexType, String)
   */
  protected static String getRegex(JobContext job, int sequence, RegexType type) {
    String key = null;
    switch (type) {
      case ROW:
        key = ROW_REGEX;
        break;
      case COLUMN_FAMILY:
        key = COLUMN_FAMILY_REGEX;
        break;
      case COLUMN_QUALIFIER:
        key = COLUMN_QUALIFIER_REGEX;
        break;
      case VALUE:
        key = VALUE_REGEX;
        break;
      default:
        throw new NoSuchElementException();
    }
    try {
      String s = job.getConfiguration().get(merge(key, sequence));
      if (s == null)
        return null;
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Failed to decode regular expression", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * @deprecated Use {@link #getFetchedColumns(Configuration)} instead
   */
  protected static Set<Pair<Text,Text>> getFetchedColumns(JobContext job) {
    return getFetchedColumns(job.getConfiguration());
  }

  /**
   * Gets the columns to be mapped over from this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a set of columns
   * @see #fetchColumns(Configuration, Collection)
   */
  protected static Set<Pair<Text,Text>> getFetchedColumns(Configuration conf) {
    return getFetchedColumns(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the columns to be mapped over from this configuration object.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a set of columns
   * @see #fetchColumns(Configuration, Collection)
   */
  protected static Set<Pair<Text,Text>> getFetchedColumns(Configuration conf, int sequence) {
    Set<Pair<Text,Text>> columns = new HashSet<Pair<Text,Text>>();
    for (String col : conf.getStringCollection(merge(COLUMNS, sequence))) {
      int idx = col.indexOf(":");
      Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes()) : Base64.decodeBase64(col.substring(0, idx).getBytes()));
      Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
      columns.add(new Pair<Text,Text>(cf, cq));
    }
    return columns;
  }

  /**
   * @deprecated Use {@link #getAutoAdjustRanges(Configuration)} instead
   */
  protected static boolean getAutoAdjustRanges(JobContext job) {
    return getAutoAdjustRanges(job.getConfiguration());
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if auto-adjust is enabled, false otherwise
   * @see #disableAutoAdjustRanges(Configuration)
   */
  protected static boolean getAutoAdjustRanges(Configuration conf) {
    return getAutoAdjustRanges(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Determines whether a configuration has auto-adjust ranges enabled.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return true if auto-adjust is enabled, false otherwise
   * @see #disableAutoAdjustRanges(Configuration)
   */
  protected static boolean getAutoAdjustRanges(Configuration conf, int sequence) {
    return conf.getBoolean(merge(AUTO_ADJUST_RANGES, sequence), true);
  }

  /**
   * @deprecated Use {@link #getLogLevel(Configuration)} instead
   */
  protected static Level getLogLevel(JobContext job) {
    return getLogLevel(job.getConfiguration());
  }

  /**
   * Gets the log level from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the log level
   * @see #setLogLevel(Configuration, Level)
   */
  protected static Level getLogLevel(Configuration conf) {
    return getLogLevel(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the log level from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the log level
   * @see #setLogLevel(Configuration, Level)
   */
  protected static Level getLogLevel(Configuration conf, int sequence) {
    return Level.toLevel(conf.getInt(merge(LOGLEVEL, sequence), Level.INFO.toInt()));
  }

  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
  /**
   * @deprecated Use {@link #validateOptions(Configuration)} instead
   */
  protected static void validateOptions(JobContext job) throws IOException {
    validateOptions(job.getConfiguration());
  }

  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @throws IOException
   *           if the configuration is improperly configured
   */
  protected static void validateOptions(Configuration conf) throws IOException {
    validateOptions(conf, DEFAULT_SEQUENCE);
  }

  // InputFormat doesn't have the equivalent of OutputFormat's
  // checkOutputSpecs(JobContext job)
  /**
   * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @throws IOException
   *           if the configuration is improperly configured
   */
  protected static void validateOptions(Configuration conf, int sequence) throws IOException {
    if (!conf.getBoolean(merge(INPUT_INFO_HAS_BEEN_SET, sequence), false))
      throw new IOException("Input info for sequence " + sequence + " has not been set.");
    if (!conf.getBoolean(merge(INSTANCE_HAS_BEEN_SET, sequence), false))
      throw new IOException("Instance info for sequence " + sequence + " has not been set.");
    // validate that we can connect as configured
    try {
      Connector c = getInstance(conf, sequence).getConnector(getUsername(conf, sequence), getPassword(conf, sequence));
      if (!c.securityOperations().authenticateUser(getUsername(conf, sequence), getPassword(conf, sequence)))
        throw new IOException("Unable to authenticate user");
      if (!c.securityOperations().hasTablePermission(getUsername(conf, sequence), getTablename(conf, sequence), TablePermission.READ))
        throw new IOException("Unable to access table");

      if (!usesLocalIterators(conf, sequence)) {
        // validate that any scan-time iterators can be loaded by the the tablet servers
        for (AccumuloIterator iter : getIterators(conf, sequence)) {
          if (!c.instanceOperations().testClassLoad(iter.getIteratorClass(), SortedKeyValueIterator.class.getName()))
            throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass() + " as a " + SortedKeyValueIterator.class.getName());
        }
      }

    } catch (AccumuloException e) {
      throw new IOException(e);
    } catch (AccumuloSecurityException e) {
      throw new IOException(e);
    }
  }

  /**
   * @deprecated Use {@link #getMaxVersions(Configuration)} instead
   */
  protected static int getMaxVersions(JobContext job) {
    return getMaxVersions(job.getConfiguration());
  }

  /**
   * Gets the maxVersions to use for the {@link VersioningIterator} from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the max versions, -1 if not configured
   * @see #setMaxVersions(Configuration, int)
   */
  protected static int getMaxVersions(Configuration conf) {
    setDefaultSequenceUsed(conf);
    return getMaxVersions(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets the maxVersions to use for the {@link VersioningIterator} from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return the max versions, -1 if not configured
   * @see #setMaxVersions(Configuration, int)
   */
  protected static int getMaxVersions(Configuration conf, int sequence) {
    return conf.getInt(merge(MAX_VERSIONS, sequence), -1);
  }

  protected static boolean isOfflineScan(Configuration conf) {
    return isOfflineScan(conf, DEFAULT_SEQUENCE);
  }

  protected static boolean isOfflineScan(Configuration conf, int sequence) {
    return conf.getBoolean(merge(READ_OFFLINE, sequence), false);
  }

  // Return a list of the iterator settings (for iterators to apply to a scanner)

  /**
   * @deprecated Use {@link #getIterators(Configuration)} instead
   */
  protected static List<AccumuloIterator> getIterators(JobContext job) {
    return getIterators(job.getConfiguration());
  }

  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterators
   * @see #addIterator(Configuration, IteratorSetting)
   */
  protected static List<AccumuloIterator> getIterators(Configuration conf) {
    return getIterators(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterators
   * @see #addIterator(Configuration, IteratorSetting)
   */
  protected static List<AccumuloIterator> getIterators(Configuration conf, int sequence) {

    String iterators = conf.get(merge(ITERATORS, sequence));

    // If no iterators are present, return an empty list
    if (iterators == null || iterators.isEmpty())
      return new ArrayList<AccumuloIterator>();

    // Compose the set of iterators encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(conf.get(merge(ITERATORS, sequence)), ITERATORS_DELIM);
    List<AccumuloIterator> list = new ArrayList<AccumuloIterator>();
    while (tokens.hasMoreTokens()) {
      String itstring = tokens.nextToken();
      list.add(new AccumuloIterator(itstring));
    }
    return list;
  }

  /**
   * @deprecated Use {@link #getIteratorOptions(Configuration)} instead
   */
  protected static List<AccumuloIteratorOption> getIteratorOptions(JobContext job) {
    return getIteratorOptions(job.getConfiguration());
  }

  /**
   * Gets a list of the iterator options specified on this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterator options
   * @see #addIterator(Configuration, IteratorSetting)
   */
  protected static List<AccumuloIteratorOption> getIteratorOptions(Configuration conf) {
    return getIteratorOptions(conf, DEFAULT_SEQUENCE);
  }

  /**
   * Gets a list of the iterator options specified on this configuration.
   * 
   * @param conf
   *          the Hadoop configuration object
   * @return a list of iterator options
   * @see #addIterator(Configuration, IteratorSetting)
   */
  protected static List<AccumuloIteratorOption> getIteratorOptions(Configuration conf, int sequence) {
    String iteratorOptions = conf.get(merge(ITERATORS_OPTIONS, sequence));

    // If no options are present, return an empty list
    if (iteratorOptions == null || iteratorOptions.isEmpty())
      return new ArrayList<AccumuloIteratorOption>();

    // Compose the set of options encoded in the job configuration
    StringTokenizer tokens = new StringTokenizer(conf.get(merge(ITERATORS_OPTIONS, sequence)), ITERATORS_DELIM);
    List<AccumuloIteratorOption> list = new ArrayList<AccumuloIteratorOption>();
    while (tokens.hasMoreTokens()) {
      String optionString = tokens.nextToken();
      list.add(new AccumuloIteratorOption(optionString));
    }
    return list;
  }

  protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V> {
    protected long numKeysRead;
    protected Iterator<Entry<Key,Value>> scannerIterator;
    private boolean scannerRegexEnabled = false;
    protected RangeInputSplit split;
    protected int sequence;

    /**
     * @deprecated since 1.4, configure {@link org.apache.accumulo.core.iterators.user.RegExFilter} instead.
     */
    private void checkAndEnableRegex(String regex, Scanner scanner, String methodName) throws IllegalArgumentException, SecurityException,
        IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException {
      if (regex != null) {
        if (scannerRegexEnabled == false) {
          scanner.setupRegex(PREFIX + ".regex.iterator", 50);
          scannerRegexEnabled = true;
        }
        scanner.getClass().getMethod(methodName, String.class).invoke(scanner, regex);
        log.info("Setting " + methodName + " to " + regex);
      }
    }

    /**
     * @deprecated since 1.4, configure {@link org.apache.accumulo.core.iterators.user.RegExFilter} instead.
     */
    protected boolean setupRegex(TaskAttemptContext attempt, Scanner scanner) throws AccumuloException {
      try {
        checkAndEnableRegex(getRegex(attempt, RegexType.ROW), scanner, "setRowRegex");
        checkAndEnableRegex(getRegex(attempt, RegexType.COLUMN_FAMILY), scanner, "setColumnFamilyRegex");
        checkAndEnableRegex(getRegex(attempt, RegexType.COLUMN_QUALIFIER), scanner, "setColumnQualifierRegex");
        checkAndEnableRegex(getRegex(attempt, RegexType.VALUE), scanner, "setValueRegex");
        return true;
      } catch (Exception e) {
        throw new AccumuloException("Can't set up regex for scanner");
      }
    }

    // Apply the configured iterators from the job to the scanner
    /**
     * @deprecated Use {@link #setupIterators(Configuration,Scanner)} instead
     */
    protected void setupIterators(TaskAttemptContext attempt, Scanner scanner) throws AccumuloException {
      setupIterators(attempt.getConfiguration(), DEFAULT_SEQUENCE, scanner);
    }

    /**
     * Apply the configured iterators from the configuration to the scanner.
     * 
     * @param conf
     *          the Hadoop configuration object
     * @param scanner
     *          the scanner to configure
     * @throws AccumuloException
     */
    protected void setupIterators(Configuration conf, int sequence, Scanner scanner) throws AccumuloException {
      List<AccumuloIterator> iterators = getIterators(conf, sequence);
      List<AccumuloIteratorOption> options = getIteratorOptions(conf, sequence);

      Map<String,IteratorSetting> scanIterators = new HashMap<String,IteratorSetting>();
      for (AccumuloIterator iterator : iterators) {
        scanIterators.put(iterator.getIteratorName(), new IteratorSetting(iterator.getPriority(), iterator.getIteratorName(), iterator.getIteratorClass()));
      }
      for (AccumuloIteratorOption option : options) {
        scanIterators.get(option.iteratorName).addOption(option.getKey(), option.getValue());
      }
      for (AccumuloIterator iterator : iterators) {
        scanner.addScanIterator(scanIterators.get(iterator.getIteratorName()));
      }
    }

    /**
     * @deprecated Use {@link #setupMaxVersions(Configuration,Scanner)} instead
     */
    protected void setupMaxVersions(TaskAttemptContext attempt, Scanner scanner) {
      setupMaxVersions(attempt.getConfiguration(), DEFAULT_SEQUENCE, scanner);
    }

    /**
     * If maxVersions has been set, configure a {@link VersioningIterator} at priority 0 for this scanner.
     * 
     * @param conf
     *          the Hadoop configuration object
     * @param scanner
     *          the scanner to configure
     */
    protected void setupMaxVersions(Configuration conf, int sequence, Scanner scanner) {
      int maxVersions = getMaxVersions(conf, sequence);
      // Check to make sure its a legit value
      if (maxVersions >= 1) {
        IteratorSetting vers = new IteratorSetting(0, "vers", VersioningIterator.class);
        VersioningIterator.setMaxVersions(vers, maxVersions);
        scanner.addScanIterator(vers);
      }
    }

    /**
     * Initialize a scanner over the given input split using this task attempt configuration.
     */
    public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
      Scanner scanner;
      split = (RangeInputSplit) inSplit;
      sequence = split.getSequence();
      log.debug("Initializing input split: " + split.range);
      Configuration conf = attempt.getConfiguration();
      Instance instance = getInstance(conf, sequence);
      String user = getUsername(conf, sequence);
      byte[] password = getPassword(conf, sequence);
      Authorizations authorizations = getAuthorizations(conf, sequence);

      try {
        log.debug("Creating connector with user: " + user);
        Connector conn = instance.getConnector(user, password);
        log.debug("Creating scanner for table: " + getTablename(conf, sequence));
        log.debug("Authorizations are: " + authorizations);
        if (isOfflineScan(conf, sequence)) {
          scanner = new OfflineScanner(instance, new AuthInfo(user, ByteBuffer.wrap(password), instance.getInstanceID()), Tables.getTableId(instance,
              getTablename(conf, sequence)), authorizations);
        } else {
          scanner = conn.createScanner(getTablename(conf, sequence), authorizations);
        }
        if (isIsolated(conf, sequence)) {
          log.info("Creating isolated scanner");
          scanner = new IsolatedScanner(scanner);
        }
        if (usesLocalIterators(conf, sequence)) {
          log.info("Using local iterators");
          scanner = new ClientSideIteratorScanner(scanner);
        }
        setupMaxVersions(conf, sequence, scanner);

        final String rowRegex = conf.get(merge(ROW_REGEX, sequence)), colfRegex = conf.get(merge(COLUMN_FAMILY_REGEX, sequence)), colqRegex = conf.get(merge(
            COLUMN_QUALIFIER_REGEX, sequence)), valueRegex = conf.get(merge(VALUE_REGEX, sequence));

        if (rowRegex != null || colfRegex != null || colqRegex != null || valueRegex != null) {
          IteratorSetting is = new IteratorSetting(50, RegExFilter.class);
          RegExFilter.setRegexs(is, rowRegex, colfRegex, colqRegex, valueRegex, false);
          scanner.addScanIterator(is);
        }
        setupIterators(conf, sequence, scanner);
      } catch (Exception e) {
        throw new IOException(e);
      }

      // setup a scanner within the bounds of this split
      for (Pair<Text,Text> c : getFetchedColumns(conf, sequence)) {
        if (c.getSecond() != null) {
          log.debug("Fetching column " + c.getFirst() + ":" + c.getSecond());
          scanner.fetchColumn(c.getFirst(), c.getSecond());
        } else {
          log.debug("Fetching column family " + c.getFirst());
          scanner.fetchColumnFamily(c.getFirst());
        }
      }

      scanner.setRange(split.range);

      numKeysRead = 0;

      // do this last after setting all scanner options
      scannerIterator = scanner.iterator();
    }

    public void close() {}

    public float getProgress() throws IOException {
      if (numKeysRead > 0 && currentKey == null)
        return 1.0f;
      return split.getProgress(currentKey);
    }

    protected K currentK = null;
    protected V currentV = null;
    protected Key currentKey = null;
    protected Value currentValue = null;

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return currentK;
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return currentV;
    }
  }

  Map<String,Map<KeyExtent,List<Range>>> binOfflineTable(JobContext job, int sequence, String tableName, List<Range> ranges) throws TableNotFoundException,
      AccumuloException, AccumuloSecurityException {

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    Configuration conf = job.getConfiguration();

    Instance instance = getInstance(conf, sequence);
    Connector conn = instance.getConnector(getUsername(conf, sequence), getPassword(conf, sequence));
    String tableId = Tables.getTableId(instance, tableName);

    if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
      Tables.clearCache(instance);
      if (Tables.getTableState(instance, tableId) != TableState.OFFLINE) {
        throw new AccumuloException("Table is online " + tableName + "(" + tableId + ") cannot scan table in offline mode ");
      }
    }

    for (Range range : ranges) {
      Text startRow;

      if (range.getStartKey() != null)
        startRow = range.getStartKey().getRow();
      else
        startRow = new Text();

      Range metadataRange = new Range(new KeyExtent(new Text(tableId), startRow, null).getMetadataEntry(), true, null, false);
      Scanner scanner = conn.createScanner(Constants.METADATA_TABLE_NAME, Constants.NO_AUTHS);
      ColumnFQ.fetch(scanner, Constants.METADATA_PREV_ROW_COLUMN);
      scanner.fetchColumnFamily(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY);
      scanner.fetchColumnFamily(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY);
      scanner.fetchColumnFamily(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY);
      scanner.setRange(metadataRange);

      RowIterator rowIter = new RowIterator(scanner);

      // TODO check that extents match prev extent

      KeyExtent lastExtent = null;

      while (rowIter.hasNext()) {
        Iterator<Entry<Key,Value>> row = rowIter.next();
        String last = "";
        KeyExtent extent = null;
        String location = null;

        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();

          if (key.getColumnFamily().equals(Constants.METADATA_LAST_LOCATION_COLUMN_FAMILY)) {
            last = entry.getValue().toString();
          }

          if (key.getColumnFamily().equals(Constants.METADATA_CURRENT_LOCATION_COLUMN_FAMILY)
              || key.getColumnFamily().equals(Constants.METADATA_FUTURE_LOCATION_COLUMN_FAMILY)) {
            location = entry.getValue().toString();
          }

          if (Constants.METADATA_PREV_ROW_COLUMN.hasColumns(key)) {
            extent = new KeyExtent(key.getRow(), entry.getValue());
          }

        }

        if (location != null)
          return null;

        if (!extent.getTableId().toString().equals(tableId)) {
          throw new AccumuloException("Saw unexpected table Id " + tableId + " " + extent);
        }

        if (lastExtent != null && !extent.isPreviousExtent(lastExtent)) {
          throw new AccumuloException(" " + lastExtent + " is not previous extent " + extent);
        }

        Map<KeyExtent,List<Range>> tabletRanges = binnedRanges.get(last);
        if (tabletRanges == null) {
          tabletRanges = new HashMap<KeyExtent,List<Range>>();
          binnedRanges.put(last, tabletRanges);
        }

        List<Range> rangeList = tabletRanges.get(extent);
        if (rangeList == null) {
          rangeList = new ArrayList<Range>();
          tabletRanges.put(extent, rangeList);
        }

        rangeList.add(range);

        if (extent.getEndRow() == null || range.afterEndKey(new Key(extent.getEndRow()).followingKey(PartialKey.ROW))) {
          break;
        }

        lastExtent = extent;
      }

    }

    return binnedRanges;
  }

  /**
   * Read the metadata table to get tablets and match up ranges to them.
   */
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    final Configuration conf = job.getConfiguration();
    final int sequence = nextSequenceToProcess(conf);
    
    if (-1 == sequence) {
      log.debug("No more splits to process");
      return Collections.emptyList();
    }

    log.setLevel(getLogLevel(conf, sequence));
    validateOptions(conf, sequence);

    String tableName = getTablename(conf, sequence);
    boolean autoAdjust = getAutoAdjustRanges(conf, sequence);
    List<Range> ranges = autoAdjust ? Range.mergeOverlapping(getRanges(conf, sequence)) : getRanges(conf, sequence);

    if (ranges.isEmpty()) {
      ranges = new ArrayList<Range>(1);
      ranges.add(new Range());
    }

    // get the metadata information for these ranges
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    TabletLocator tl;
    try {
      if (isOfflineScan(conf, sequence)) {
        binnedRanges = binOfflineTable(job, sequence, tableName, ranges);
        while (binnedRanges == null) {
          // Some tablets were still online, try again
          UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
          binnedRanges = binOfflineTable(job, sequence, tableName, ranges);
        }
      } else {
        Instance instance = getInstance(conf, sequence);
        String tableId = null;
        tl = getTabletLocator(conf, sequence);
        // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
        tl.invalidateCache();
        while (!tl.binRanges(ranges, binnedRanges).isEmpty()) {
          if (!(instance instanceof MockInstance)) {
            if (tableId == null)
              tableId = Tables.getTableId(instance, tableName);
            if (!Tables.exists(instance, tableId))
              throw new TableDeletedException(tableId);
            if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
              throw new TableOfflineException(instance, tableId);
          }
          binnedRanges.clear();
          log.warn("Unable to locate bins for specified ranges. Retrying.");
          UtilWaitThread.sleep(100 + (int) (Math.random() * 100)); // sleep randomly between 100 and 200 ms
          tl.invalidateCache();
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    ArrayList<InputSplit> splits = new ArrayList<InputSplit>(ranges.size());
    HashMap<Range,ArrayList<String>> splitsToAdd = null;

    if (!autoAdjust)
      splitsToAdd = new HashMap<Range,ArrayList<String>>();

    HashMap<String,String> hostNameCache = new HashMap<String,String>();

    for (Entry<String,Map<KeyExtent,List<Range>>> tserverBin : binnedRanges.entrySet()) {
      String ip = tserverBin.getKey().split(":", 2)[0];
      String location = hostNameCache.get(ip);
      if (location == null) {
        InetAddress inetAddress = InetAddress.getByName(ip);
        location = inetAddress.getHostName();
        hostNameCache.put(ip, location);
      }

      for (Entry<KeyExtent,List<Range>> extentRanges : tserverBin.getValue().entrySet()) {
        Range ke = extentRanges.getKey().toDataRange();
        for (Range r : extentRanges.getValue()) {
          if (autoAdjust) {
            // divide ranges into smaller ranges, based on the
            // tablets
            splits.add(new RangeInputSplit(tableName, ke.clip(r), new String[] {location}, sequence));
          } else {
            // don't divide ranges
            ArrayList<String> locations = splitsToAdd.get(r);
            if (locations == null)
              locations = new ArrayList<String>(1);
            locations.add(location);
            splitsToAdd.put(r, locations);
          }
        }
      }
    }

    if (!autoAdjust)
      for (Entry<Range,ArrayList<String>> entry : splitsToAdd.entrySet())
        splits.add(new RangeInputSplit(tableName, entry.getKey(), entry.getValue().toArray(new String[0]), sequence));
    
    log.info("Returning splits:" + splits);
    return splits;
  }

  /**
   * The Class RangeInputSplit. Encapsulates an Accumulo range for use in Map Reduce jobs.
   */
  public static class RangeInputSplit extends InputSplit implements Writable {
    private Range range;
    private String[] locations;
    private int sequence;

    public RangeInputSplit() {
      range = new Range();
      locations = new String[0];
    }

    public Range getRange() {
      return range;
    }

    public int getSequence() {
      return sequence;
    }

    private static byte[] extractBytes(ByteSequence seq, int numBytes) {
      byte[] bytes = new byte[numBytes + 1];
      bytes[0] = 0;
      for (int i = 0; i < numBytes; i++) {
        if (i >= seq.length())
          bytes[i + 1] = 0;
        else
          bytes[i + 1] = seq.byteAt(i);
      }
      return bytes;
    }

    public static float getProgress(ByteSequence start, ByteSequence end, ByteSequence position) {
      int maxDepth = Math.min(Math.max(end.length(), start.length()), position.length());
      BigInteger startBI = new BigInteger(extractBytes(start, maxDepth));
      BigInteger endBI = new BigInteger(extractBytes(end, maxDepth));
      BigInteger positionBI = new BigInteger(extractBytes(position, maxDepth));
      return (float) (positionBI.subtract(startBI).doubleValue() / endBI.subtract(startBI).doubleValue());
    }

    public float getProgress(Key currentKey) {
      if (currentKey == null)
        return 0f;
      if (range.getStartKey() != null && range.getEndKey() != null) {
        if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW) != 0) {
          // just look at the row progress
          return getProgress(range.getStartKey().getRowData(), range.getEndKey().getRowData(), currentKey.getRowData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM) != 0) {
          // just look at the column family progress
          return getProgress(range.getStartKey().getColumnFamilyData(), range.getEndKey().getColumnFamilyData(), currentKey.getColumnFamilyData());
        } else if (range.getStartKey().compareTo(range.getEndKey(), PartialKey.ROW_COLFAM_COLQUAL) != 0) {
          // just look at the column qualifier progress
          return getProgress(range.getStartKey().getColumnQualifierData(), range.getEndKey().getColumnQualifierData(), currentKey.getColumnQualifierData());
        }
      }
      // if we can't figure it out, then claim no progress
      return 0f;
    }

    RangeInputSplit(String table, Range range, String[] locations) {
      this(table, range, locations, DEFAULT_SEQUENCE);
    }

    RangeInputSplit(String table, Range range, String[] locations, int sequence) {
      this.range = range;
      this.locations = locations;
      this.sequence = sequence;
    }

    /**
     * This implementation of length is only an estimate, it does not provide exact values. Do not have your code rely on this return value.
     */
    public long getLength() throws IOException {
      Text startRow = range.isInfiniteStartKey() ? new Text(new byte[] {Byte.MIN_VALUE}) : range.getStartKey().getRow();
      Text stopRow = range.isInfiniteStopKey() ? new Text(new byte[] {Byte.MAX_VALUE}) : range.getEndKey().getRow();
      int maxCommon = Math.min(7, Math.min(startRow.getLength(), stopRow.getLength()));
      long diff = 0;

      byte[] start = startRow.getBytes();
      byte[] stop = stopRow.getBytes();
      for (int i = 0; i < maxCommon; ++i) {
        diff |= 0xff & (start[i] ^ stop[i]);
        diff <<= Byte.SIZE;
      }

      if (startRow.getLength() != stopRow.getLength())
        diff |= 0xff;

      return diff + 1;
    }

    public String[] getLocations() throws IOException {
      return locations;
    }

    public void readFields(DataInput in) throws IOException {
      range.readFields(in);
      sequence = in.readInt();
      int numLocs = in.readInt();
      locations = new String[numLocs];
      for (int i = 0; i < numLocs; ++i)
        locations[i] = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
      range.write(out);
      out.writeInt(sequence);
      out.writeInt(locations.length);
      for (int i = 0; i < locations.length; ++i)
        out.writeUTF(locations[i]);
    }
  }

  /**
   * The Class IteratorSetting. Encapsulates specifics for an Accumulo iterator's name & priority.
   */
  static class AccumuloIterator {

    private static final String FIELD_SEP = ":";

    private int priority;
    private String iteratorClass;
    private String iteratorName;

    public AccumuloIterator(int priority, String iteratorClass, String iteratorName) {
      this.priority = priority;
      this.iteratorClass = iteratorClass;
      this.iteratorName = iteratorName;
    }

    // Parses out a setting given an string supplied from an earlier toString() call
    public AccumuloIterator(String iteratorSetting) {
      // Parse the string to expand the iterator
      StringTokenizer tokenizer = new StringTokenizer(iteratorSetting, FIELD_SEP);
      priority = Integer.parseInt(tokenizer.nextToken());
      iteratorClass = tokenizer.nextToken();
      iteratorName = tokenizer.nextToken();
    }

    public int getPriority() {
      return priority;
    }

    public String getIteratorClass() {
      return iteratorClass;
    }

    public String getIteratorName() {
      return iteratorName;
    }

    @Override
    public String toString() {
      return new String(priority + FIELD_SEP + iteratorClass + FIELD_SEP + iteratorName);
    }

  }

  /**
   * The Class AccumuloIteratorOption. Encapsulates specifics for an Accumulo iterator's optional configuration details - associated via the iteratorName.
   */
  static class AccumuloIteratorOption {
    private static final String FIELD_SEP = ":";

    private String iteratorName;
    private String key;
    private String value;

    public AccumuloIteratorOption(String iteratorName, String key, String value) {
      this.iteratorName = iteratorName;
      this.key = key;
      this.value = value;
    }

    // Parses out an option given a string supplied from an earlier toString() call
    public AccumuloIteratorOption(String iteratorOption) {
      StringTokenizer tokenizer = new StringTokenizer(iteratorOption, FIELD_SEP);
      this.iteratorName = tokenizer.nextToken();
      try {
        this.key = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
        this.value = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    public String getIteratorName() {
      return iteratorName;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      try {
        return new String(iteratorName + FIELD_SEP + URLEncoder.encode(key, "UTF-8") + FIELD_SEP + URLEncoder.encode(value, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

  }

}
