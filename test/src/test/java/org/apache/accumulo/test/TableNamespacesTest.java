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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNamespaceNotEmptyException;
import org.apache.accumulo.core.client.impl.TableNamespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TableNamespacesTest {
  
  Random random = new Random();
  public static TemporaryFolder folder = new TemporaryFolder();
  static private MiniAccumuloCluster accumulo;
  static private String secret = "secret";
  
  @BeforeClass
  static public void setUp() throws Exception {
    folder.create();
    accumulo = new MiniAccumuloCluster(folder.getRoot(), secret);
    accumulo.start();
  }
  
  @AfterClass
  static public void tearDown() throws Exception {
    accumulo.stop();
    folder.delete();
  }
  
  /**
   * This test creates a table without specifying a namespace. In this case, it puts the table into the default namespace.
   * 
   * @throws Exception
   */
  @Test
  public void testDefaultNamespace() throws Exception {
    String tableName = "test";
    Connector c = accumulo.getConnector("root", secret);
    
    assertTrue(c.tableNamespaceOperations().exists(Constants.DEFAULT_TABLE_NAMESPACE));
    c.tableOperations().create(tableName);
    assertTrue(c.tableOperations().exists(tableName));
  }
  
  /**
   * This test creates a new namespace "testing" and a table "testing.table1" which puts "table1" into the "testing" namespace. Then we create "testing.table2"
   * which creates "table2" and puts it into "testing" as well. Then we make sure that you can't delete a namespace with tables in it, and then we delete the
   * tables and delete the namespace.
   * 
   * @throws Exception
   */
  @Test
  public void testCreateAndDeleteNamespace() throws Exception {
    String namespace = "testing";
    String tableName1 = namespace + ".table1";
    String tableName2 = namespace + ".table2";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableNamespaceOperations().create(namespace);
    assertTrue(c.tableNamespaceOperations().exists(namespace));
    
    c.tableOperations().create(tableName1);
    assertTrue(c.tableOperations().exists(tableName1));
    
    c.tableOperations().create(tableName2);
    assertTrue(c.tableOperations().exists(tableName2));
    
    // deleting
    try {
      // can't delete a namespace with tables in it
      c.tableNamespaceOperations().delete(namespace);
      fail();
    } catch (TableNamespaceNotEmptyException e) {
      // ignore, supposed to happen
    }
    assertTrue(c.tableNamespaceOperations().exists(namespace));
    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(c.tableOperations().exists(tableName2));
    
    c.tableOperations().delete(tableName2);
    assertTrue(!c.tableOperations().exists(tableName2));
    assertTrue(c.tableNamespaceOperations().exists(namespace));
    
    c.tableOperations().delete(tableName1);
    assertTrue(!c.tableOperations().exists(tableName1));
    c.tableNamespaceOperations().delete(namespace);
    assertTrue(!c.tableNamespaceOperations().exists(namespace));
  }
  
  /**
   * This test creates a namespace, modifies it's properties, and checks to make sure that those properties are applied to its tables. To do something on a
   * namespace-wide level, use TableNamespaceOperations.
   * 
   * Checks to make sure namespace-level properties are overridden by table-level properties.
   * 
   * Checks to see if the default namespace's properties work as well.
   * 
   * @throws Exception
   */
  
  @Test
  public void testNamespaceProperties() throws Exception {
    String namespace = "propchange";
    String tableName1 = namespace + ".table1";
    String tableName2 = namespace + ".table2";
    
    String propKey = Property.TABLE_SCAN_MAXMEM.getKey();
    String propVal = "42K";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableNamespaceOperations().create(namespace);
    c.tableOperations().create(tableName1);
    c.tableNamespaceOperations().setProperty(namespace, propKey, propVal);
    
    // check the namespace has the property
    boolean itWorked = false;
    for (Entry<String,String> prop : c.tableNamespaceOperations().getProperties(namespace)) {
      if (prop.getKey().equals(propKey) && prop.getValue().equals(propVal)) {
        itWorked = true;
        break;
      }
    }
    
    assertTrue(itWorked);
    
    // check that the table gets it from the namespace
    itWorked = false;
    for (Entry<String,String> prop : c.tableOperations().getProperties(tableName1)) {
      if (prop.getKey().equals(propKey) && prop.getValue().equals(propVal)) {
        itWorked = true;
        break;
      }
    }
    assertTrue(itWorked);
    
    // test a second table to be sure the first wasn't magical
    // (also, changed the order, the namespace already exists with the property)
    itWorked = false;
    c.tableOperations().create(tableName2);
    for (Entry<String,String> prop : c.tableOperations().getProperties(tableName2)) {
      if (prop.getKey().equals(propKey) && prop.getValue().equals(propVal)) {
        itWorked = true;
        break;
      }
    }
    assertTrue(itWorked);
    
    // test that table properties override namespace properties
    String propKey2 = Property.TABLE_FILE_MAX.getKey();
    String propVal2 = "42";
    String tablePropVal = "13";
    
    c.tableOperations().setProperty(tableName2, propKey2, tablePropVal);
    c.tableNamespaceOperations().setProperty("propchange", propKey2, propVal2);
    
    itWorked = false;
    for (Entry<String,String> prop : c.tableOperations().getProperties(tableName2)) {
      if (prop.getKey().equals(propKey2) && prop.getValue().equals(tablePropVal)) {
        itWorked = true;
        break;
      }
    }
    assertTrue(itWorked);
    
    // now check that you can change the default namespace's properties
    propVal = "13K";
    propVal2 = "44";
    String tableName = "some_table";
    c.tableOperations().create(tableName);
    c.tableNamespaceOperations().setProperty(Constants.DEFAULT_TABLE_NAMESPACE, propKey, propVal);
    
    itWorked = false;
    for (Entry<String,String> prop : c.tableOperations().getProperties(tableName)) {
      if (prop.getKey().equals(propKey) && prop.getValue().equals(propVal)) {
        itWorked = true;
        break;
      }
    }
    assertTrue(itWorked);
  }
  
  /**
   * This test creates a new user and a namespace. It checks to make sure the user can't modify anything in the namespace at first, then it grants the user
   * permissions and makes sure that they can modify the namespace. Then it also checks if the user has the correct permissions on tables both already existing
   * in the namespace and ones they create.
   * 
   * @throws Exception
   */
  @Test
  public void testNamespacePermissions() throws Exception {
    // TODO make the test once namespace-level permissions are implemented. (ACCUMULO-1479)
  }
  
  /**
   * This test renames and clones two separate table into different namespaces. different namespace.
   * 
   * @throws Exception
   */
  @Test
  public void testRenameAndCloneTableToNewNamespace() throws Exception {
    String namespace1 = "renamed";
    String namespace2 = "cloned";
    String tableName = "table";
    String tableName1 = "renamed.table1";
    String tableName2 = "cloned.table2";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableOperations().create(tableName);
    c.tableNamespaceOperations().create(namespace1);
    c.tableNamespaceOperations().create(namespace2);
    
    c.tableOperations().rename(tableName, tableName1);
    
    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(!c.tableOperations().exists(tableName));
    
    c.tableOperations().clone(tableName1, tableName2, false, null, null);
    
    assertTrue(c.tableOperations().exists(tableName1));
    assertTrue(c.tableOperations().exists(tableName2));
    return;
  }
  
  /**
   * This test renames a table namespace and ensures that its tables are still correct
   */
  @Test
  public void testNamespaceRename() throws Exception {
    String namespace1 = "n1";
    String namespace2 = "n2";
    String table = "t";
    
    Connector c = accumulo.getConnector("root", secret);
    Instance instance = c.getInstance();
    
    c.tableNamespaceOperations().create(namespace1);
    c.tableOperations().create(namespace1 + "." + table);
    
    c.tableNamespaceOperations().rename(namespace1, namespace2);
    
    assertTrue(!c.tableNamespaceOperations().exists(namespace1));
    assertTrue(c.tableNamespaceOperations().exists(namespace2));
    assertTrue(c.tableOperations().exists(namespace2 + "." + table));
    String tid = Tables.getTableId(instance, namespace2 + "." + table);
    String tnid = Tables.getNamespace(instance, tid);
    String tnamespace = TableNamespaces.getNamespaceName(instance, tnid);
    assertTrue(namespace2.equals(tnamespace));
  }
  
  /**
   * This test clones a table to a different namespace and ensures it's properties are correct
   */
  @Test
  public void testCloneTableProperties() throws Exception {
    String n1 = "namespace1";
    String n2 = "namespace2";
    String t1 = n1 + ".table";
    String t2 = n2 + ".table";
    
    String propKey = Property.TABLE_FILE_MAX.getKey();
    String propVal1 = "55";
    String propVal2 = "66";
    
    Connector c = accumulo.getConnector("root", secret);
    
    c.tableNamespaceOperations().create(n1);
    c.tableOperations().create(t1);
    
    c.tableOperations().removeProperty(t1, Property.TABLE_FILE_MAX.getKey());
    c.tableNamespaceOperations().setProperty(n1, propKey, propVal1);
    
    boolean itWorked = false;
    for (Entry<String,String> prop : c.tableOperations().getProperties(t1)) {
      if (prop.getKey().equals(propKey) && prop.getValue().equals(propVal1)) {
        itWorked = true;
        break;
      }
    }
    assertTrue(itWorked);
    
    c.tableNamespaceOperations().create(n2);
    c.tableNamespaceOperations().setProperty(n2, propKey, propVal2);
    c.tableOperations().clone(t1, t2, true, null, null);
    c.tableOperations().removeProperty(t2, propKey);
    
    itWorked = false;
    for (Entry<String,String> prop : c.tableOperations().getProperties(t2)) {
      if (prop.getKey().equals(propKey) && prop.getValue().equals(propVal2)) {
        itWorked = true;
        break;
      }
    }
    assertTrue(itWorked);
  }
}