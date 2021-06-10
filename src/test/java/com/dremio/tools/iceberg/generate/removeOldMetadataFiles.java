package com.dremio.tools.iceberg.generate;

import com.dremio.tools.iceberg.util.TableTestBase;
import org.apache.iceberg.*;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;

import static org.apache.iceberg.types.Conversions.toByteBuffer;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

public class removeOldMetadataFiles extends TableTestBase {
    private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get())
    );


    private static SparkSession spark = null;

    @BeforeClass
    public static void startSpark(){
        removeOldMetadataFiles.spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();
    }

    @AfterClass
    public static void stopSpark() {
        SparkSession currentSpark = removeOldMetadataFiles.spark;
        removeOldMetadataFiles.spark = null;
        currentSpark.stop();
    }

    @Before
    public void setUp() {
        tableDir = Paths.get("generated-tables").resolve("iceberg-table-" + LocalTime.now().toString().replaceAll(":", "-")).toAbsolutePath().normalize().toFile();
        System.out.println("Using table directory: " + tableDir);
        boolean delete = tableDir.delete();
        table = create(SCHEMA, SPEC);
    }

    @Test
    public void compactSmallDataFiles(){
        table.updateProperties()
                .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,"true")
                .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,"5")
                .commit();
        int fileCount = 10;
        System.out.println("Appending " + fileCount + " files to the table...");
        for (int i = 0; i < fileCount; i++) {
            DataFile dataFile = buildDataFileWithMetricsForStringColumn(100 + i);
            table.newAppend()
                    .appendFile(dataFile)
                    .commit();
        }
        Iterable<DataFile> files = FindFiles.in(table).collect();
        assertEquals(fileCount, pathSet(files).size());
        assertEquals(fileCount, Sets.newHashSet(table.snapshots()).size());
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
