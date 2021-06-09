package com.dremio.tools.iceberg.generate;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Paths;
import java.security.Principal;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.*;
import org.apache.iceberg.actions.ExpireSnapshotsActionResult;
import org.apache.iceberg.actions.RewriteManifestsActionResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;

import com.dremio.tools.iceberg.util.TableTestBase;

/**
 * Generates tables containing partitions
 */
public class RewriteManifestFilesDemo extends TableTestBase {
    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get())
    );
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .bucket("id", 16)
            .build();

    public final DataFile FILE_0 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=0")
            .withPath(new File(tableDir, "data-0.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(100)
            .build();

    public final DataFile FILE_1 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=1")
            .withPath(new File(tableDir, "data-1.parquet").toString())
            .withFileSizeInBytes(100)
            .withRecordCount(100)
            .build();

    public final DataFile FILE_2 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=2")
            .withPath(new File(tableDir, "data-2.parquet").toString())
            .withFileSizeInBytes(400)
            .withRecordCount(1000)
            .build();

    private static SparkSession spark = null;

    @BeforeClass
    public static void startSpark(){
        RewriteManifestFilesDemo.spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();
    }

    @AfterClass
    public static void stopSpark() {
        SparkSession currentSpark = RewriteManifestFilesDemo.spark;
        RewriteManifestFilesDemo.spark = null;
        currentSpark.stop();
    }

    @Before
    public void setUp() {
        tableDir = Paths.get("iceberg-table-14-59-26.330").toAbsolutePath().normalize().toFile();
        Configuration CONF = new Configuration();
        HadoopTables TABLES = new HadoopTables(CONF);
        String location = "/home/plr/iceberg-generator-main/generated-tables/iceberg-table-14-59-26.330";
        table = TABLES.load(location);
    }

    @Test
    public void reducingNoOfManifestFiles(){
        List<ManifestFile> manifests = table.currentSnapshot().allManifests();
        table.updateProperties()
                .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES,String.valueOf((long)(manifests.get(1).length() * 3)))
                .commit();
        Actions act = Actions.forTable(table);
        RewriteManifestsActionResult result = act.rewriteManifests()
            .execute();
        table.refresh();
    }
}