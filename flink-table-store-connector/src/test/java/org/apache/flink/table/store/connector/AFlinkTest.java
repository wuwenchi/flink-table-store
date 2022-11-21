package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.codegen.CodeGenLoader;
import org.apache.flink.table.store.connector.sink.FlinkSinkBuilder;
import org.apache.flink.table.store.connector.source.FlinkSourceBuilder;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.CoreOptions.BUCKET;
import static org.apache.flink.table.store.CoreOptions.FILE_FORMAT;
import static org.apache.flink.table.store.CoreOptions.PATH;
import static org.apache.flink.table.store.CoreOptions.WRITE_MODE;
import static org.apache.flink.table.store.file.WriteMode.CHANGE_LOG;


/*
    运行时，需要 codegen 类，该类可以直接编译出来：
    1. 把编译出来的 flink-table-store-codegen-0.3-SNAPSHOT.jar 重命名为 flink-table-store-codegen.jar,
    2. 放到 flink-table-store-connector/target/classes/ 下。
 */

public class AFlinkTest {

  @Before
  public void before() {
    parameter();
  }

  public static void parameter() {
    dbName = "db";
    tbName = "tb5";
    parallelism = 1;
    isStream = false;

    dbPath = dbName + ".db";
    warehouse = "file:///Users/wuwenchi/github/flink-table-store/warehouse/";
    tableLocation = "file:///Users/wuwenchi/github/flink-table-store/warehouse/" + dbPath + "/" + tbName;
    IDENTIFIER = ObjectIdentifier.of("catalog", dbName, tbName);
    getFlinkEnv();
  }

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  public static final RowType TABLE_TYPE =
      new RowType(
          Arrays.asList(
              new RowType.RowField("id1", new IntType()),
              new RowType.RowField("id2", new IntType()),
              new RowType.RowField("par1", new VarCharType()),
              new RowType.RowField("par2", new VarCharType()),
              new RowType.RowField("value", new VarCharType())
          ));

  public static String dbName;
  public static String dbPath;
  public static String tbName;
  public static int parallelism = 1;
  public static boolean isStream;
  public static StreamExecutionEnvironment env;
  public static StreamTableEnvironment tEnv;
  public static String tableLocation;
  public static String warehouse;

  public static ObjectIdentifier IDENTIFIER;

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static final DataStructureConverter<RowData, Row> CONVERTER =
      (DataStructureConverter)
          DataStructureConverters.getConverter(
              TypeConversions.fromLogicalToDataType(TABLE_TYPE));

  public static final List<RowData> SOURCE_DATA =
      Arrays.asList(
          getGenRowData("+I", 1, 1, "a", "v3"),
          getGenRowData("+I", 1, 2, "a", "v3"),
          getGenRowData("+I", 4, 1, "a", "v3"),
          getGenRowData("+I", 3, 2, "a", "v4"));

  public static SerializableRowData getGenRowData(String opt, int id1, int id2, String par1, String value) {
    RowKind kind = null;
    switch (opt) {
      case "+I":
        kind = RowKind.INSERT;
        break;
      case "-D":
        kind = RowKind.DELETE;
        break;
      case "-U":
        kind = RowKind.UPDATE_BEFORE;
        break;
      case "+U":
        kind = RowKind.UPDATE_AFTER;
        break;
    }
    GenericRowData rowData = GenericRowData.ofKind(kind, id1, id2, StringData.fromString(par1), StringData.fromString("b"), StringData.fromString(value));
    return new SerializableRowData(rowData, InternalSerializers.create(TABLE_TYPE));
  }

  public static void getFlinkEnv() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    if (isStream) {
      env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    } else {
      env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    }
    env.setParallelism(parallelism);

    tEnv = StreamTableEnvironment.create(env);
    tEnv.executeSql("" +
        "CREATE CATALOG my_catalog WITH (\n" +
        "  'type'='table-store',\n" +
        "  'warehouse'='" + warehouse + "'\n" +
        ");\n" +
        "");

    tEnv.executeSql("USE CATALOG my_catalog;");
    tEnv.executeSql("USE " + dbName);
  }

  @Test
  public void createBySql() {
    tEnv.executeSql("" +
        "CREATE TABLE IF NOT EXISTS " + tbName + " (\n" +
        "   id1 INT,\n" +
        "   id2 INT,\n" +
        "   par1 STRING,\n" +
        "   par2 STRING,\n" +
        "   val STRING,\n" +
        "   PRIMARY KEY (id1, par1) NOT ENFORCED\n" +
        ") \n" +
        "PARTITIONED BY (par1) \n" +
        "WITH ( " + getCfgString() + ") \n" +
        "");
    tEnv.executeSql("" +
        "INSERT INTO " + tbName + " VALUES \n" +
        "(7, 6, 'a', 'b', 'v1')," +
        "(5, 7, 'a', 'b', 'v1')" +
        ";");

    tEnv.executeSql("SELECT * FROM " + tbName).print();
  }


  public static Configuration getConf() {
    Configuration options = new Configuration();
    options.set(BUCKET, 1);
    options.set(PATH, tableLocation);
    options.set(FILE_FORMAT, "avro");
    options.set(WRITE_MODE, CHANGE_LOG);
    return options;
  }

  public String getCfgString() {
    ArrayList<String> strings = new ArrayList<>();
    getConf().toMap().forEach(
        (k, v) -> {
          String stringBuilder = "'" + k + "'" +
              " = " +
              "'" + v + "'\n";
          strings.add(stringBuilder);
        }
    );
    return String.join(", ", strings);
  }

  public static FileStoreTable buildFileStoreTable(
      int[] partitions, int[] primaryKey)
      throws Exception {
    Configuration options = getConf();
    Path tablePath = new CoreOptions(options).path();
    UpdateSchema updateSchema =
        new UpdateSchema(
            TABLE_TYPE,
            Arrays.stream(partitions)
                .mapToObj(i -> TABLE_TYPE.getFieldNames().get(i))
                .collect(Collectors.toList()),
            Arrays.stream(primaryKey)
                .mapToObj(i -> TABLE_TYPE.getFieldNames().get(i))
                .collect(Collectors.toList()),
            options.toMap(),
            "");

    new SchemaManager(tablePath).commitNewVersion(updateSchema);
    return FileStoreTableFactory.create(options);
  }

  @Test
  public void createFileStoreTable() throws Exception {
    int[] partitions = {2};
    int[] primaryKey = {0, 2};
    FileStoreTable table = buildFileStoreTable(partitions, primaryKey);

    // write
    new FlinkSinkBuilder(IDENTIFIER, table).withInput(env.addSource(new SourceData(), InternalTypeInfo.of(TABLE_TYPE))).build();
    env.execute();

    // read
    CloseableIterator<RowData> iterator = new FlinkSourceBuilder(IDENTIFIER, table).withEnv(env).build().executeAndCollect();
    List<Row> results = new ArrayList<>();
    while (iterator.hasNext()) {
      results.add(CONVERTER.toExternal(iterator.next()));
    }
    iterator.close();
    results.forEach(System.out::println);
  }

  @Test
  public void testClassLoader() {
    String FLINK_TABLE_STORE_CODEGEN_FAT_JAR = "flink-table-store-codegen-loader-0.3-SNAPSHOT.jar";
//    FLINK_TABLE_STORE_CODEGEN_FAT_JAR = "/target/flink-table-store-connector-0.3-SNAPSHOT.jar";
    ClassLoader classLoader = CodeGenLoader.class.getClassLoader();
    InputStream resourceAsStream = classLoader.getResourceAsStream(FLINK_TABLE_STORE_CODEGEN_FAT_JAR);
    System.out.println(resourceAsStream);
  }

  public static class SourceData implements SourceFunction<RowData> {

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
      SOURCE_DATA.forEach(sourceContext::collect);
    }

    @Override
    public void cancel() {

    }
  }

  @Test
  public void te12() {
    Date date = new Date(1668069242000L);
    System.out.println(date);
  }
}
