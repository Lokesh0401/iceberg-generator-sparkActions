package com.dremio.tools.iceberg.generate;

import com.dremio.tools.iceberg.util.TableTestBase;
import org.apache.iceberg.*;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.List;

import static org.apache.iceberg.types.Types.NestedField.required;

public class RewriteManifestFilesWithoutSpark extends TableTestBase {
    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get())
    );
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .bucket("id", 2)
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
            .withPartitionPath("id_bucket=0")
            .withPath(new File(tableDir, "data-2.parquet").toString())
            .withFileSizeInBytes(400)
            .withRecordCount(1000)
            .build();

    public final DataFile FILE_3 = DataFiles.builder(SPEC)
            .withPartitionPath("id_bucket=0")
            .withPath(new File(tableDir, "data-3.parquet").toString())
            .withFileSizeInBytes(400)
            .withRecordCount(1000)
            .build();

    @Before
    public void setUp() {
        tableDir = Paths.get("generated-tables").resolve("iceberg-table-" + LocalTime.now().toString().replaceAll(":", "-")).toAbsolutePath().normalize().toFile();
        System.out.println("Using table directory: " + tableDir);
        boolean delete = tableDir.delete();
        table = create(SCHEMA, SPEC);
    }

    @Test
    public void reClusterManifestFiles(){
        table.newAppend()
                .appendFile(FILE_0)
                .appendFile(FILE_1)
                .commit();

        table.newAppend()
                .appendFile(FILE_2)
                .appendFile(FILE_3)
                .commit();
        table.rewriteManifests()
                .rewriteIf(manifestFile -> true)
                .clusterBy(manifestFile -> manifestFile.partition().get(0,Integer.class))
                .commit();
        List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests();
        Assert.assertEquals(2,manifestFiles.size());
        long count1 = manifestFiles.get(0).existingFilesCount();
        long count2 = manifestFiles.get(1).existingFilesCount();
        long count = count1+count2;
        Assert.assertEquals(4,count);
        Assert.assertTrue(count1==3 || count1==1);
    }
}
