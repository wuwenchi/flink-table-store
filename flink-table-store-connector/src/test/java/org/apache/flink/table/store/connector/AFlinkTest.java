package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
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
import org.apache.flink.table.store.file.WriteMode;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

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
    isStream = true;

    dbPath = dbName + ".db";
    warehouse = "file:///Users/wuwenchi/github/flink-table-store/warehouse/";
    tableLocation = warehouse + dbPath + "/" + tbName;
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
  public static int parallelism;
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
          getGenRowData("-D", 4, 2, "a", "v3"),
          getGenRowData("-D", 3, 1, "a", "v3"),
          getGenRowData("-D", 3, 2, "a", "v4"));

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
    ArrayList<String> strings = new ArrayList<>();
    strings.add("(3, 1, 'a', 'b', 'v1')");
    strings.add("(2, 1, 'a', 'b', 'v1')");
    strings.add("(1, 1, 'a', 'b', 'v1')");
    strings.add("(9, 1, 'a', 'b', 'v1')");
    strings.add("(5, 1, 'a', 'b', 'v1')");

    strings.add("(13, 2, 'a', 'b', 'v1')");
    strings.add("(2, 2, 'a', 'b', 'v1')");
    strings.add("(1, 2, 'a', 'b', 'v1')");
    strings.add("(9, 2, 'a', 'b', 'v1')");
//    strings.add("(6, 7, 'a', 'b', 'v1')");
//    strings.add("(1, 7, 'a', 'b', 'v1')");
//    tEnv.executeSql("" +
//        "INSERT INTO " + tbName + " VALUES \n" +
//        "(7, 6, 'a', 'b', 'v1')," +
//        "(3, 7, 'a', 'b', 'v1')" +
//        ";");
    strings.forEach(s -> {
      tEnv.executeSql("" +
        "INSERT INTO " + tbName + " VALUES " + s + ";");
    });

    tEnv.executeSql("SELECT * FROM " + tbName).print();
  }

  @Test
  public void re() {
    tEnv.executeSql("" +
        "CREATE TABLE IF NOT EXISTS " + tbName + " (\n" +
        "   id1 INT,\n" +
        "   id2 INT,\n" +
        "   par1 STRING,\n" +
        "   par2 STRING,\n" +
        "   val STRING\n" +
//        "   PRIMARY KEY (id1, par1) NOT ENFORCED\n" +
        ") \n" +
        "PARTITIONED BY (par1) \n" +
        "WITH ( " + getCfgString() + ") \n" +
        "");
    ArrayList<String> strings = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      strings.add("(" + i + ", 1, 'a', 'adf;bwei;qeivjq;eivjw;oijadsivja;sdoivja', 'v;eina;wiejgawvna;weifawefaefjoijwaef;sdf')");
    }
//    strings.forEach(s -> {
//      tEnv.executeSql("" +
//        "INSERT INTO " + tbName + " VALUES " + s + ";");
//    });
//    tEnv.executeSql("" +
//        "INSERT INTO " + tbName + " VALUES (1,1,'a','a','b'),(2,1,'a','a','b') ;");

    tEnv.executeSql("SELECT * FROM " + tbName).print();
//    tEnv.executeSql("SELECT * FROM " + tbName+"$snapshots").print();
//    tEnv.executeSql("SELECT * FROM " + tbName+"$options").print();
  }


  public static Configuration getConf() {
    Configuration opt = new Configuration();
    opt.set(CoreOptions.BUCKET, 1);
    opt.set(CoreOptions.PATH, tableLocation);
    opt.set(CoreOptions.FILE_FORMAT, "avro");
    opt.set(CoreOptions.WRITE_MODE, WriteMode.CHANGE_LOG);
//    opt.set(WRITE_MODE, WriteMode.APPEND_ONLY);

    opt.setBoolean(CoreOptions.COMMIT_FORCE_COMPACT, true);
    // append only compaction
    opt.setInteger(CoreOptions.COMPACTION_MIN_FILE_NUM, 1);
    opt.setInteger(CoreOptions.COMPACTION_MAX_FILE_NUM, 2);
    opt.set(CoreOptions.TARGET_FILE_SIZE, MemorySize.parse("1 kb"));

    // changelog compaction
    opt.setInteger(CoreOptions.NUM_LEVELS, 10);
    opt.setInteger(CoreOptions.COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT, 10000);

    // producer   default:none
    opt.set(CoreOptions.CHANGELOG_PRODUCER, CoreOptions.ChangelogProducer.NONE);
    opt.set(CoreOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL, Duration.ofSeconds(1));
    return opt;
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
