package com.dremio.tools.iceberg.generate;

import com.dremio.tools.iceberg.util.TableTestBase;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.RewriteManifestsActionResult;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.required;

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

    public final DataFile FILE_3 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=3")
            .withPath(new File(tableDir, "data-3.parquet").toString())
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
        tableDir = Paths.get("generated-tables").resolve("iceberg-table-" + LocalTime.now().toString().replaceAll(":", "-")).toAbsolutePath().normalize().toFile();
        System.out.println("Using table directory: " + tableDir);
        boolean delete = tableDir.delete();
        table = create(SCHEMA, SPEC);
    }

    @Test
    public void reducingNoOfManifestFiles(){
        table.newAppend()
                .appendFile(FILE_0)
                .appendFile(FILE_1)
                .commit();

        table.newAppend()
                .appendFile(FILE_2)
                .appendFile(FILE_3)
                .commit();

        List<ManifestFile> manifests = table.currentSnapshot().allManifests();
        Assert.assertEquals(2,manifests.size());
        table.updateProperties()
                .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES,String.valueOf((manifests.get(0).length() * 3)))
                .commit();
        Actions act = Actions.forTable(table);
        RewriteManifestsActionResult result = act.rewriteManifests()
                .rewriteIf(manifestFile -> true)
                .execute();
        table.refresh();
        manifests = table.currentSnapshot().allManifests();
        Assert.assertEquals(1,manifests.size());
    }
}