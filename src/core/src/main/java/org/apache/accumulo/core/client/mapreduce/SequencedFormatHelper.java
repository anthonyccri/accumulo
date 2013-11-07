package org.apache.accumulo.core.client.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Convenience class with methods useful to dealing with multiple configurations of AccumuloInputFormat and/or AccumuloOutputFormat in the same Configuration
 * object.
 */
public class SequencedFormatHelper {

  private static final String COMMA = ",";
  private static final String TRUE = "true";
  public static final int DEFAULT_SEQUENCE = 0;

  public static final String DEFAULT_SEQ_USED = ".defaultSequenceUsed";
  public static final String CONFIGURED_SEQUENCES = ".configuredSeqs";
  public static final String PROCESSED_SEQUENCES = ".processedSeqs";

  /**
   * Get a unique identifier for these configurations
   * 
   * @return A unique number to provide to future AccumuloInputFormat calls
   */
  public static synchronized int nextSequence(Configuration conf, String prefix) {
    ArgumentChecker.notNull(conf, prefix);

    final String configuredSequences = prefix + CONFIGURED_SEQUENCES;

    String value = conf.get(configuredSequences);
    if (null == value) {
      conf.set(configuredSequences, "1");
      return 1;
    } else {
      String[] splitValues = StringUtils.split(value, COMMA);
      int newValue = Integer.parseInt(splitValues[splitValues.length - 1]) + 1;

      conf.set(configuredSequences, value + COMMA + newValue);
      return newValue;
    }
  }
  
  /**
   * Returns all configured sequences but not the default sequence
   * @param conf
   * @param prefix
   * @return
   */
  public static Integer[] configuredSequences(Configuration conf, String prefix) {
    ArgumentChecker.notNull(conf, prefix);
    
    final String configuredSequences = prefix + CONFIGURED_SEQUENCES;
    String[] values = conf.getStrings(configuredSequences);
    if (null == values) {
      return new Integer[0];
    }
    
    Integer[] intValues = new Integer[values.length];
    for (int i = 0; i < values.length; i++) {
      intValues[i] = Integer.parseInt(values[i]);
    }
    
    return intValues;
  }

  protected static boolean isDefaultSequenceUsed(Configuration conf, String prefix) {
    ArgumentChecker.notNull(conf, prefix);
    
    final String defaultSequenceUsedKey = prefix + DEFAULT_SEQ_USED;
    
    return conf.getBoolean(defaultSequenceUsedKey, false);
  }
  
  protected static void setDefaultSequenceUsed(Configuration conf, String prefix) {
    ArgumentChecker.notNull(conf, prefix);

    final String defaultSequenceUsedKey = prefix + DEFAULT_SEQ_USED;

    String value = conf.get(defaultSequenceUsedKey);
    if (null == value || !TRUE.equals(value)) {
      conf.setBoolean(defaultSequenceUsedKey, true);
    }
  }

  /**
   * Using the provided Configuration, return the next sequence number to process.
   * 
   * @param conf
   *          A Configuration object used to store AccumuloInputFormat information into
   * @return The next sequence number to process, -1 when finished.
   * @throws NoSuchElementException
   */
  protected static synchronized int nextSequenceToProcess(Configuration conf, String prefix) throws NoSuchElementException {
    ArgumentChecker.notNull(prefix);

    final String processedSequences = prefix + PROCESSED_SEQUENCES, defaultSequenceUsed = prefix + DEFAULT_SEQ_USED, configuredSequences = prefix
        + CONFIGURED_SEQUENCES;

    String[] processedConfs = conf.getStrings(processedSequences);

    // We haven't set anything, so we need to find the first to return
    if (null == processedConfs || 0 == processedConfs.length) {
      // Check to see if the default sequence was used
      boolean defaultSeqUsed = conf.getBoolean(defaultSequenceUsed, false);

      // If so, set that we're processing it and return the value of the default
      if (defaultSeqUsed) {
        conf.set(processedSequences, Integer.toString(DEFAULT_SEQUENCE));
        return DEFAULT_SEQUENCE;
      }

      String[] loadedConfs = conf.getStrings(configuredSequences);

      // There was *nothing* loaded, fail.
      if (null == loadedConfs || 0 == loadedConfs.length) {
        throw new NoSuchElementException("Sequence was requested to process but none exist to return");
      }

      // We have loaded configuration(s), use the first
      int firstLoaded = Integer.parseInt(loadedConfs[0]);
      conf.setInt(processedSequences, firstLoaded);

      return firstLoaded;
    }

    // We've previously parsed some confs, need to find the next one to load
    int lastProcessedSeq = Integer.valueOf(processedConfs[processedConfs.length - 1]);
    String[] configuredSequencesArray = conf.getStrings(configuredSequences);

    // We only have the default sequence, no specifics.
    // Getting here, we already know that we processed that default
    if (null == configuredSequencesArray) {
      return -1;
    }

    List<Integer> configuredSequencesList = new ArrayList<Integer>(configuredSequencesArray.length + 1);

    // If we used the default sequence ID, add that into the list of configured sequences
    if (conf.getBoolean(defaultSequenceUsed, false)) {
      configuredSequencesList.add(DEFAULT_SEQUENCE);
    }

    // Add the rest of any sequences to our list
    for (String configuredSequence : configuredSequencesArray) {
      configuredSequencesList.add(Integer.parseInt(configuredSequence));
    }

    int lastParsedSeqIndex = configuredSequencesList.size() - 1;

    // Find the next sequence number after the one we last processed
    for (; lastParsedSeqIndex >= 0; lastParsedSeqIndex--) {
      int lastLoadedValue = configuredSequencesList.get(lastParsedSeqIndex);

      if (lastLoadedValue == lastProcessedSeq) {
        break;
      }
    }

    // We either had no sequences to match or we matched the last configured sequence
    // Both of which are equivalent to no (more) sequences to process
    if (-1 == lastParsedSeqIndex || lastParsedSeqIndex + 1 >= configuredSequencesList.size()) {
      return -1;
    }

    // Get the value of the sequence at that offset
    int nextSequence = configuredSequencesList.get(lastParsedSeqIndex + 1);
    conf.set(processedSequences, conf.get(processedSequences) + COMMA + nextSequence);

    return nextSequence;
  }
}
