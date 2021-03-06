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
package org.apache.accumulo.server;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ServerConstants {
  
  public static final String VERSION_DIR = "version";

  public static final String INSTANCE_ID_DIR = "instance_id";

  /**
   * current version (3) reflects additional namespace operations (ACCUMULO-802) in version 1.6.0 <br />
   * (versions should never be negative)
   */
  public static final Integer WIRE_VERSION = 3;
  
  /**
   * current version (6) reflects the addition of a separate root table (ACCUMULO-1481) in version 1.6.0
   */
  public static final int DATA_VERSION = 6;
  public static final int PREV_DATA_VERSION = 5;
  
  private static String[] baseDirs = null;
  private static String defaultBaseDir = null;

  public static synchronized String getDefaultBaseDir() {
    if (defaultBaseDir == null) {
      String singleNamespace = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_DIR);
      String dfsUri = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_URI);
      String baseDir;
      
      if (dfsUri == null || dfsUri.isEmpty()) {
        Configuration hadoopConfig = CachedConfiguration.getInstance();
        try {
          baseDir = FileSystem.get(hadoopConfig).getUri().toString() + singleNamespace;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        if (!dfsUri.contains(":"))
          throw new IllegalArgumentException("Expected fully qualified URI for " + Property.INSTANCE_DFS_URI.getKey() + " got " + dfsUri);
        baseDir = dfsUri + singleNamespace;
      }
      
      defaultBaseDir = new Path(baseDir).toString();
      
    }
    
    return defaultBaseDir;
  }

  public static String[] getConfiguredBaseDirs() {
    String singleNamespace = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_DFS_DIR);
    String ns = ServerConfiguration.getSiteConfiguration().get(Property.INSTANCE_VOLUMES);

    String configuredBaseDirs[];

    if (ns == null || ns.isEmpty()) {
      configuredBaseDirs = new String[] {getDefaultBaseDir()};
    } else {
      String namespaces[] = ns.split(",");
      for (String namespace : namespaces) {
        if (!namespace.contains(":")) {
          throw new IllegalArgumentException("Expected fully qualified URI for " + Property.INSTANCE_VOLUMES.getKey() + " got " + namespace);
        }
      }
      configuredBaseDirs = prefix(namespaces, singleNamespace);
    }

    return configuredBaseDirs;
  }

  // these are functions to delay loading the Accumulo configuration unless we must
  public static synchronized String[] getBaseDirs() {
    if (baseDirs == null) {
      baseDirs = checkBaseDirs(getConfiguredBaseDirs(), false);
    }
    

    return baseDirs;
  }

  public static String[] checkBaseDirs(String[] configuredBaseDirs, boolean ignore) {
    // all base dirs must have same instance id and data version, any dirs that have neither should be ignored
    String firstDir = null;
    String firstIid = null;
    Integer firstVersion = null;
    ArrayList<String> baseDirsList = new ArrayList<String>();
    for (String baseDir : configuredBaseDirs) {
      Path path = new Path(baseDir, INSTANCE_ID_DIR);
      String currentIid;
      Integer currentVersion;
      try {
        currentIid = ZooUtil.getInstanceIDFromHdfs(new Path(baseDir, INSTANCE_ID_DIR));
        Path vpath = new Path(baseDir, VERSION_DIR);
        currentVersion = Accumulo.getAccumuloPersistentVersion(vpath.getFileSystem(CachedConfiguration.getInstance()), vpath);
      } catch (Exception e) {
        if (ignore)
          continue;
        else
          throw new IllegalArgumentException("Accumulo volume " + path + " not initialized", e);
      }

      if (firstIid == null) {
        firstIid = currentIid;
        firstDir = baseDir;
        firstVersion = currentVersion;
      } else if (!currentIid.equals(firstIid)) {
        throw new IllegalArgumentException("Configuration " + Property.INSTANCE_VOLUMES.getKey() + " contains paths that have different instance ids "
            + baseDir + " has " + currentIid + " and " + firstDir + " has " + firstIid);
      } else if (!currentVersion.equals(firstVersion)) {
        throw new IllegalArgumentException("Configuration " + Property.INSTANCE_VOLUMES.getKey() + " contains paths that have different versions " + baseDir
            + " has " + currentVersion + " and " + firstDir + " has " + firstVersion);
      }

      baseDirsList.add(baseDir);
    }

    if (baseDirsList.size() == 0) {
      throw new RuntimeException("None of the configured paths are initialized.");
    }

    return baseDirsList.toArray(new String[baseDirsList.size()]);
  }
  
  public static String[] prefix(String bases[], String suffix) {
    if (suffix.startsWith("/"))
      suffix = suffix.substring(1);
    String result[] = new String[bases.length];
    for (int i = 0; i < bases.length; i++) {
      result[i] = bases[i] + "/" + suffix;
    }
    return result;
  }
  
  public static final String TABLE_DIR = "tables";
  public static final String RECOVERY_DIR = "recovery";
  public static final String WAL_DIR = "wal";

  public static String[] getTablesDirs() {
    return prefix(getBaseDirs(), TABLE_DIR);
  }

  public static String[] getRecoveryDirs() {
    return prefix(getBaseDirs(), RECOVERY_DIR);
  }
  
  public static String[] getWalDirs() {
    return prefix(getBaseDirs(), WAL_DIR);
  }
  
  public static String[] getWalogArchives() {
    return prefix(getBaseDirs(), "walogArchive");
  }
  
  public static Path getInstanceIdLocation() {
    // all base dirs should have the same instance id, so can choose any one
    return new Path(getBaseDirs()[0], INSTANCE_ID_DIR);
  }
  
  public static Path getDataVersionLocation() {
    // all base dirs should have the same version, so can choose any one
    return new Path(getBaseDirs()[0], VERSION_DIR);
  }
  

  public static String[] getMetadataTableDirs() {
    return prefix(getTablesDirs(), MetadataTable.ID);
  }
  
  public static String[] getTemporaryDirs() {
    return prefix(getBaseDirs(), "tmp");
  }

}
