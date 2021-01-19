package com.dremio.tools.iceberg.generate;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

import com.dremio.tools.iceberg.util.TableTestBase;

/**
 * Generates tables containing partitions
 */
public class PartitionedTableGenerator extends TableTestBase {
  private static final Schema SCHEMA = new Schema(
    required(1, "id", Types.IntegerType.get()),
    required(2, "data", Types.StringType.get())
  );
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
    .bucket("id", 16)
    .build();

  private final DataFile FILE_0 = DataFiles.builder(SPEC)
    .withPartitionPath("id_bucket=0")
    .withPath(new File(tableDir, "data-0.parquet").toString())
    .withFileSizeInBytes(10)
    .withRecordCount(100)
    .build();

  private final DataFile FILE_1 = DataFiles.builder(SPEC)
    .withPartitionPath("id_bucket=1")
    .withPath(new File(tableDir, "data-1.parquet").toString())
    .withFileSizeInBytes(100)
    .withRecordCount(1000)
    .build();

  @Before
  public void setUp() {
    tableDir = Paths.get("generated-tables").resolve("iceberg-table-" + LocalTime.now().toString().replaceAll(":", "-")).toAbsolutePath().normalize().toFile();
    System.out.println("Using table directory: " + tableDir);
    tableDir.delete();
    table = create(SCHEMA, SPEC);
  }

  @Test
  public void createPartitionedTable() {
    table.newAppend()
      .appendFile(FILE_0)
      .appendFile(FILE_1)
      .commit();

    Iterable<DataFile> files = FindFiles.in(table)
      .inPartition(SPEC, FILE_0.partition())
      .collect();
    assertEquals(pathSet(FILE_0), pathSet(files));
  }
}
