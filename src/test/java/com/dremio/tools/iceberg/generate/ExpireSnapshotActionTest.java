package com.dremio.tools.iceberg.generate;

import com.dremio.tools.iceberg.util.TableTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.ExpireSnapshotsActionResult;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

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
        Configuration CONF = new Configuration();
        HadoopTables TABLES = new HadoopTables(CONF);
        String location = "/home/plr/iceberg-generator-sparkActions/generated-tables/iceberg-table-16-19-53.617188";
        table = TABLES.load(location);
    }

    @Test
    public void expireSnapshotsOlderThanHour(){
        long tsToExpire = System.currentTimeMillis()- 1000*60*60;
        Actions act = Actions.forTable(table);
        ExpireSnapshotsActionResult exa =  act.expireSnapshots().expireOlderThan(tsToExpire).execute();
        table.refresh();
    }
}