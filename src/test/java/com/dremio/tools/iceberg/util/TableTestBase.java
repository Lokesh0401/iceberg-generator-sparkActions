package com.dremio.tools.iceberg.util;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public class TableTestBase {
  protected File tableDir = null;
  protected Table table = null;

  protected Table create(Schema schema, PartitionSpec spec) {
    requireNonNull(tableDir, "tableDir not set in the test");
    return new HadoopTables(new Configuration()).create(schema, spec, SortOrder.unsorted(), ImmutableMap.of(), tableDir.toString());
  }

  protected Set<String> pathSet(DataFile... files) {
    return Sets.newHashSet(Iterables.transform(Arrays.asList(files), file -> file.path().toString()));
  }

  protected Set<String> pathSet(Iterable<DataFile> files) {
    return Sets.newHashSet(Iterables.transform(files, file -> file.path().toString()));
  }
}
