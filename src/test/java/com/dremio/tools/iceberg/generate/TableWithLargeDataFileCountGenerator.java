package com.dremio.tools.iceberg.generate;

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

import com.dremio.tools.iceberg.util.TableTestBase;

/**
 * Generates tables with a high number of data files
 */
public class TableWithLargeDataFileCountGenerator extends TableTestBase {
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final Schema SCHEMA = new Schema(
    required(1, "id", Types.IntegerType.get()),
    required(2, "data", Types.StringType.get())
  );

  @Before
  public void setUp() {
    tableDir = Paths.get("generated-tables").resolve("iceberg-table-" + LocalTime.now().toString().replaceAll(":", "-")).toAbsolutePath().normalize().toFile();
    System.out.println("Using table directory: " + tableDir);
    tableDir.delete();
    table = create(SCHEMA, SPEC);
  }

  @Test
  public void createTableWithLargeDataFileAndSnapshotCount() {
    int fileCount = 1000_000;
    System.out.println("Appending " + fileCount + " files to the table...");
    for (int i = 0; i < fileCount; i++) {
      DataFile dataFile = buildDataFileWithMetricsForStringColumn(100 + i);
      table.newAppend()
        .appendFile(dataFile)
        .commit();
    }

    Iterable<DataFile> files = FindFiles.in(table).collect();
    assertEquals(fileCount, pathSet(files).size());
    assertEquals(fileCount, table.currentSnapshot().snapshotId());
  }

  private DataFile buildDataFileWithMetricsForStringColumn(long records) {
    return DataFiles.builder(SPEC)
      .withPath(new File(tableDir, "data-" + records + ".parquet").toString())
      .withFileSizeInBytes(100)
      .withMetrics(new Metrics(
        records,
        null, // no column sizes
        ImmutableMap.of(2, 3L), // value count
        ImmutableMap.of(2, 0L), // null count
        ImmutableMap.of(2, toByteBuffer(Types.StringType.get(), "a")),  // lower bounds
        ImmutableMap.of(2, toByteBuffer(Types.StringType.get(), "z")))) // upper bounds
      .build();
  }
}
