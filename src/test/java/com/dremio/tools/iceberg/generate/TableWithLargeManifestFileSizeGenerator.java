package com.dremio.tools.iceberg.generate;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

import com.dremio.tools.iceberg.util.TableTestBase;

/**
 * Generates tables with a high number of data files
 */
public class TableWithLargeManifestFileSizeGenerator extends TableTestBase {
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final Schema SCHEMA = new Schema(
    required(3, "id", Types.IntegerType.get()),
    required(4, "data", Types.StringType.get())
  );

  @Before
  public void setUp() {
    tableDir = Paths.get("generated-tables").resolve("iceberg-table-" + LocalTime.now().toString().replaceAll(":", "-")).toAbsolutePath().normalize().toFile();
    System.out.println("Using table directory: " + tableDir);
    tableDir.delete();
    table = create(SCHEMA, SPEC);
  }

  @Test
  public void createTableWithLargeManifestFile() {
    int fileCount = 100;
    System.out.println("Appending " + fileCount + " files to the table...");

    Transaction transaction = table.newTransaction();
    AppendFiles appendFiles = transaction.newAppend();
    for (int i = 0; i < fileCount; i++) {
      DataFile dataFile = buildDataFileWithMetricsForStringColumn(100 + i);
      appendFiles.appendFile(dataFile);
    }

    appendFiles.commit();
    transaction.commitTransaction();
    Iterable<DataFile> files = FindFiles.in(table).collect();
    assertEquals(fileCount, pathSet(files).size());
    assertEquals(1L, table.currentSnapshot().snapshotId());
  }

  private DataFile buildDataFileWithMetricsForStringColumn(long records) {
    return DataFiles.builder(SPEC)
      .withPath(new File(tableDir, "data-" + records + ".parquet").toString())
      .withFileSizeInBytes(100)
      .withRecordCount(records)
      .build();
  }
}
