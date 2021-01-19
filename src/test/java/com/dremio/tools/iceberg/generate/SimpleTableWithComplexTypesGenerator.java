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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Before;
import org.junit.Test;

import com.dremio.tools.iceberg.util.TableTestBase;

/**
 * Generates a simple table with complex data types
 */
public class SimpleTableWithComplexTypesGenerator extends TableTestBase {
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final Schema SCHEMA = new Schema(
    required(1, "id", IntegerType.get()),
    required(2, "elements", ListType.ofRequired(3, StringType.get())),
    required(4, "data", StructType.of(required(5, "struct", StringType.get())))
  );

  @Before
  public void setUp() {
    tableDir = Paths.get("generated-tables").resolve("iceberg-table-" + LocalTime.now().toString().replaceAll(":", "-")).toAbsolutePath().normalize().toFile();
    System.out.println("Using table directory: " + tableDir);
    tableDir.delete();
    table = create(SCHEMA, SPEC);
  }

  @Test
  public void createTableWithHundredRecords() {
    DataFile dataFile = buildDataFile(100);
    table.newAppend()
      .appendFile(dataFile)
      .commit();

    Iterable<DataFile> files = FindFiles.in(table).collect();
    assertEquals(pathSet(dataFile), pathSet(files));
  }

  @Test
  public void createTableWithMinMax() {
    DataFile dataFileOne = buildDataFileWithMetricsForIdColumn(100);
    DataFile dataFileTwo = buildDataFileWithMetricsForStringColumn(1000);
    table.newAppend()
      .appendFile(dataFileOne)
      .appendFile(dataFileTwo)
      .commit();

    Iterable<DataFile> files = FindFiles.in(table)
      .withRecordsMatching(Expressions.in("id", 1, 10, 100))
      .collect();
    assertEquals(pathSet(dataFileOne, dataFileTwo), pathSet(files));
  }

  private DataFile buildDataFile(long records) {
    return DataFiles.builder(SPEC)
      .withPath(new File(tableDir, "data.parquet").toString())
      .withFileSizeInBytes(10)
      .withRecordCount(records)
      .build();
  }

  private DataFile buildDataFileWithMetricsForIdColumn(long records) {
    return DataFiles.builder(SPEC)
      .withPath(new File(tableDir, "data.parquet").toString())
      .withFileSizeInBytes(10)
      .withMetrics(new Metrics(
        records,
        null, // no column sizes
        ImmutableMap.of(1, 3L), // value count
        ImmutableMap.of(1, 0L), // null count
        ImmutableMap.of(1, toByteBuffer(IntegerType.get(), 1)),  // lower bounds
        ImmutableMap.of(1, toByteBuffer(IntegerType.get(), 5)))) // upper bounds
      .build();
  }

  private DataFile buildDataFileWithMetricsForStringColumn(long records) {
    return DataFiles.builder(SPEC)
      .withPath(new File(tableDir, "data-2.parquet").toString())
      .withFileSizeInBytes(100)
      .withMetrics(new Metrics(
        records,
        null, // no column sizes
        ImmutableMap.of(2, 3L), // value count
        ImmutableMap.of(2, 0L), // null count
        ImmutableMap.of(2, toByteBuffer(StringType.get(), "a")),  // lower bounds
        ImmutableMap.of(2, toByteBuffer(StringType.get(), "z")))) // upper bounds
      .build();
  }
}
