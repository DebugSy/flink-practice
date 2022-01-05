package com.flink.demo.cases.case23;

import org.apache.flink.api.java.hadoop.mapred.utils.HadoopUtils;
import org.apache.flink.connectors.hive.HiveTableSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.descriptors.HiveCatalogDescriptor;
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import javax.annotation.Nullable;
import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class HiveSourceTraining {

    public static void main(String[] args) throws Exception {
        ObjectPath path = new ObjectPath("test", "shiy_lookup_t1");

        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.INT())
                .field("name", DataTypes.STRING())
                .field("address", DataTypes.STRING())
                .build();

        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogConfig.IS_GENERIC, String.valueOf(true));
        properties.put("connector", "COLLECTION");

        HiveConf hiveConf = createHiveConf(null);
        CatalogDescriptor catalogDescriptor = new HiveCatalogDescriptor();
        Map<String, String> properties1 = catalogDescriptor.toProperties();
        Catalog catalog = TableFactoryService.find(CatalogFactory.class, properties1)
                .createCatalog("test", properties);

        CatalogTable table = new CatalogTableImpl(schema, properties, "csv table");
        HiveTableSource hiveTableSource = new HiveTableSource(new JobConf(), path, table);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.registerTableSource("shiy_lookup_t1", hiveTableSource);

        Table table1 = tEnv.sqlQuery("select * from shiy_lookup_t1");
        DataStream<Row> rowDataStream = tEnv.toAppendStream(table1, Row.class);
        rowDataStream.printToErr();

        env.execute("Hive Source Training");

    }

    private static HiveConf createHiveConf(@Nullable String hiveConfDir) {
        System.err.println("Setting hive conf dir as " + hiveConfDir);

        try {
            HiveConf.setHiveSiteLocation(
                    hiveConfDir == null ?
                            null : Paths.get(hiveConfDir, "hive-site.xml").toUri().toURL());
        } catch (MalformedURLException e) {
            throw new CatalogException(
                    String.format("Failed to get hive-site.xml from %s", hiveConfDir), e);
        }

        // create HiveConf from hadoop configuration
        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(new org.apache.flink.configuration.Configuration());

        // Add mapred-site.xml. We need to read configurations like compression codec.
        for (String possibleHadoopConfPath : HadoopUtils.possibleHadoopConfPaths(new org.apache.flink.configuration.Configuration())) {
            File mapredSite = new File(new File(possibleHadoopConfPath), "mapred-site.xml");
            if (mapredSite.exists()) {
                hadoopConf.addResource(new Path(mapredSite.getAbsolutePath()));
                break;
            }
        }
        return new HiveConf(hadoopConf, HiveConf.class);
    }

}
