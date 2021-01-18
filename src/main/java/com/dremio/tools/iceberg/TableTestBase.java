/*
 * Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.tools.iceberg;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.After;

public class TableTestBase {
  protected File tableDir = null;
  protected TestTables.TestTable table = null;

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  protected TestTables.TestTable create(Schema schema, PartitionSpec spec) {
    return TestTables.create(tableDir, "test", schema, spec);
  }

  Set<String> pathSet(DataFile... files) {
    return Sets.newHashSet(Iterables.transform(Arrays.asList(files), file -> file.path().toString()));
  }

  Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }
}
