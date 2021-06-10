package com.dremio.tools.iceberg.generate;

import com.dremio.tools.iceberg.util.TableTestBase;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.ExpireSnapshotsActionResult;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;

import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Generates tables containing partitions
 */
public class ExpireSnapshotActionTest extends TableTestBase {
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
        ExpireSnapshotActionTest.spark = SparkSession.builder()
                .master("local[2]")
                .getOrCreate();
    }

    @AfterClass
    public static void stopSpark() {
        SparkSession currentSpark = ExpireSnapshotActionTest.spark;
        ExpireSnapshotActionTest.spark = null;
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
    public void expireOlderSnapshots() throws InterruptedException {
        table.newAppend()
                .appendFile(FILE_0)
                .appendFile(FILE_1)
                .commit();
        table.refresh();
        long count = 0;
        Iterable<Snapshot> snaps = table.snapshots();
        for (Snapshot snap : snaps) {
            count++;
        }
        Assert.assertEquals(1,count);
        Thread.sleep(1000*30);
        table.newAppend()
                .appendFile(FILE_2)
                .commit();
        table.refresh();
        long count1 = 0;
        Iterable<Snapshot> snaps1 = table.snapshots();
        for (Snapshot snapshot : snaps1) {
            count1++;
        }
        Assert.assertEquals(2,count1);
        long tsToExpire = System.currentTimeMillis()-1000*20;
        Actions act = Actions.forTable(table);
        ExpireSnapshotsActionResult exa =  act.expireSnapshots().expireOlderThan(tsToExpire).execute();
        table.refresh();
        long count2 = 0;
        Iterable<Snapshot> snaps2 = table.snapshots();
        for (Snapshot snapshot : snaps2) {
            count2++;
        }
        Assert.assertEquals(1,count2);

    }
}