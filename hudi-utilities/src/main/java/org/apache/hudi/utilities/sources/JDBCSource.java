package org.apache.hudi.utilities.sources;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.jetbrains.annotations.NotNull;

/**
 * Reads data from RDBMS data sources
 */

public class JDBCSource extends RowSource {

  private static Logger LOG = LogManager.getLogger(JDBCSource.class);

  public JDBCSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  /**
   * Validates all user properties and prepares the {@link DataFrameReader} to read from RDBMS
   *
   * @param session
   * @param properties
   * @return
   * @throws HoodieException
   */
  private static DataFrameReader validatePropsAndGetDataFrameReader(final SparkSession session,
      final TypedProperties properties)
      throws HoodieException {
    DataFrameReader dataFrameReader = null;
    FSDataInputStream passwordFileStream = null;
    try {
      dataFrameReader = session.read().format("jdbc");
      dataFrameReader = dataFrameReader.option(Config.URL_PROP, properties.getString(Config.URL));
      dataFrameReader = dataFrameReader.option(Config.USER_PROP, properties.getString(Config.USER));
      dataFrameReader = dataFrameReader.option(Config.DRIVER_PROP, properties.getString(Config.DRIVER_CLASS));
      dataFrameReader = dataFrameReader
          .option(Config.RDBMS_TABLE_PROP, properties.getString(Config.RDBMS_TABLE_NAME));

      if (properties.containsKey(Config.PASSWORD)) {
        LOG.info("Reading JDBC password from properties file....");
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, properties.getString(Config.PASSWORD));
      } else if (properties.containsKey(Config.PASSWORD_FILE) && !StringUtils
          .isNullOrEmpty(properties.getString(Config.PASSWORD_FILE))) {
        LOG.info(
            String.format("Reading JDBC password from password file %s", properties.getString(Config.PASSWORD_FILE)));
        FileSystem fileSystem = FileSystem.get(session.sparkContext().hadoopConfiguration());
        passwordFileStream = fileSystem.open(new Path(properties.getString(Config.PASSWORD_FILE)));
        byte[] bytes = new byte[passwordFileStream.available()];
        passwordFileStream.read(bytes);
        dataFrameReader = dataFrameReader.option(Config.PASSWORD_PROP, new String(bytes));
      } else {
        throw new IllegalArgumentException(String.format("JDBCSource needs either a %s or %s to connect to RDBMS "
            + "datasource", Config.PASSWORD_FILE, Config.PASSWORD));
      }

      addExtraJdbcOptions(properties, dataFrameReader);

      if (properties.getBoolean(Config.IS_INCREMENTAL)) {
        DataSourceUtils.checkRequiredProperties(properties, Arrays.asList(Config.INCREMENTAL_COLUMN));
      }
      return dataFrameReader;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      IOUtils.closeStream(passwordFileStream);
    }
  }

  /**
   * Accepts spark JDBC options from the user in terms of EXTRA_OPTIONS adds them to {@link DataFrameReader} Example: In
   * a normal spark code you would do something like: session.read.format('jdbc') .option(fetchSize,1000)
   * .option(timestampFormat,"yyyy-mm-dd hh:mm:ss")
   * <p>
   * The way to pass these properties to HUDI is through the config file. Any property starting with
   * hoodie.datasource.jdbc.extra.options. will be added.
   * <p>
   * Example: hoodie.datasource.jdbc.extra.options.fetchSize hoodie.datasource.jdbc.extra.options.upperBound
   * hoodie.datasource.jdbc.extra.options.lowerBound
   *
   * @param properties
   * @param dataFrameReader
   */
  private static void addExtraJdbcOptions(TypedProperties properties, DataFrameReader dataFrameReader) {
    Set<Object> objects = properties.keySet();
    for (Object property : objects) {
      String prop = (String) property;
      if (prop.startsWith(Config.EXTRA_OPTIONS)) {
        String key = Arrays.asList(prop.split(Config.EXTRA_OPTIONS)).stream()
            .collect(Collectors.joining());
        String value = properties.getString(prop);
        if (value != null) {
          LOG.info(String.format("Adding %s -> %s to jdbc options", key, value));
          dataFrameReader.option(key, value);
        } else {
          LOG.warn(String.format("Skipping %s jdbc option as value is null or empty", key));
        }
      }
    }
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    try {
      DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.URL, Config.DRIVER_CLASS, Config.USER,
          Config.RDBMS_TABLE_NAME, Config.IS_INCREMENTAL));
      Option<String> lastCheckpoint =
          lastCkptStr.isPresent() ? lastCkptStr.get().isEmpty() ? Option.empty() : lastCkptStr : Option.empty();
      return fetch(lastCheckpoint);
    } catch (Exception e) {
      LOG.error("Exception while running JDBCSource ", e);
      return Pair.of(Option.empty(), null);
    }
  }

  /**
   * Decide to do a full RDBMS table scan or an incremental scan based on the lastCheckpoint. If previous checkpoint
   * value exists then we do an incremental scan with a PPD query or else we do a full scan. In certain cases where the
   * incremental query fails, we fallback to a full scan.
   *
   * @param lastCheckpoint
   * @return
   */
  @NotNull
  private Pair<Option<Dataset<Row>>, String> fetch(Option<String> lastCheckpoint) {
    Dataset<Row> dataset;
    boolean isIncremental = props.getBoolean(Config.IS_INCREMENTAL);
    if (lastCheckpoint.equals(Option.empty()) || StringUtils.isNullOrEmpty(lastCheckpoint.get())) {
      LOG.info("No checkpoint references found... Doing a full rdbms table fetch");
      dataset = fullFetch();
    } else {
      dataset = incrementalFetch(lastCheckpoint);
    }
    dataset.persist(StorageLevel.MEMORY_AND_DISK_SER());
    Pair<Option<Dataset<Row>>, String> pair = Pair.of(Option.of(dataset), checkpoint(dataset, isIncremental));
    dataset.unpersist();
    return pair;
  }

  /**
   * Does an incremental scan with PPQ query prepared on the bases of previous checkpoint.
   *
   * @param lastCheckpoint
   * @return
   */
  @NotNull
  private Dataset<Row> incrementalFetch(Option<String> lastCheckpoint) {
    try {
      final String ppdQuery = "(select * from %s where %s >= \"%s\") rdbms_table";
      String query = String
          .format(ppdQuery, props.getString(Config.RDBMS_TABLE_NAME), props.getString(Config.INCREMENTAL_COLUMN),
              lastCheckpoint.get());
      LOG.info(String
          .format("Referenced last checkpoint and prepared new predicate pushdown query for jdbc pull %s", query));
      return validatePropsAndGetDataFrameReader(sparkSession, props)
          .option(Config.RDBMS_TABLE_PROP, query).load();
    } catch (Exception e) {
      LOG.error("Error while performing an incremental fetch... \n Note: Not all database support the PPD query we "
          + "generate to do na incremental scan", e);
      LOG.warn("Falling back to full scan.......");
      return fullFetch();
    }
  }

  /**
   * Does a full scan on the RDBMS data source
   * @return
   */
  private Dataset<Row> fullFetch() {
    return validatePropsAndGetDataFrameReader(sparkSession, props).load();
  }

  private String checkpoint(Dataset<Row> rowDataset, boolean isIncremental) {
    try {
      if (isIncremental) {
        Column incrementalColumn = rowDataset.col(props.getString(Config.INCREMENTAL_COLUMN));
        final String max = rowDataset.agg(functions.max(incrementalColumn).cast(DataTypes.StringType)).first()
            .getString(0);
        LOG.info(String.format("Checkpointing column %s with value: %s ", incrementalColumn, max));
        return max;
      } else {
        return null;
      }
    } catch (Exception e) {
      return null;
    }
  }


  protected static class Config {

    /**
     * {@value #URL} is the jdbc url for the Hoodie datasource
     */
    private static final String URL = "hoodie.datasource.jdbc.url";

    private static final String URL_PROP = "url";

    /**
     * {@value #USER} is the username used for JDBC connection
     */
    private static final String USER = "hoodie.datasource.jdbc.user";

    /**
     * {@value #USER_PROP} used internally to build jdbc params
     */
    private static final String USER_PROP = "user";

    /**
     * {@value #PASSWORD} is the password used for JDBC connection
     */
    private static final String PASSWORD = "hoodie.datasource.jdbc.password";

    /**
     * {@value #PASSWORD_FILE} is the base-path for the JDBC password file
     */
    private static final String PASSWORD_FILE = "hoodie.datasource.jdbc.password.file";

    /**
     * {@value #PASSWORD_PROP} used internally to build jdbc params
     */
    private static final String PASSWORD_PROP = "password";

    /**
     * {@value #DRIVER_CLASS} used for JDBC connection
     */
    private static final String DRIVER_CLASS = "hoodie.datasource.jdbc.driver.class";

    /**
     * {@value #DRIVER_PROP} used internally to build jdbc params
     */
    private static final String DRIVER_PROP = "driver";

    /**
     * {@value #RDBMS_TABLE_NAME} RDBMS table to pull
     */
    private static final String RDBMS_TABLE_NAME = "hoodie.datasource.jdbc.table.name";

    /**
     * {@value #RDBMS_TABLE_PROP} used internally for jdbc
     */
    private static final String RDBMS_TABLE_PROP = "dbtable";

    /**
     * {@value #INCREMENTAL_COLUMN} if ran in incremental mode, this field will be used to pull new data incrementally
     */
    private static final String INCREMENTAL_COLUMN = "hoodie.datasource.jdbc.table.incremental.column.name";

    /**
     * {@value #IS_INCREMENTAL} will the JDBC source do an incremental pull?
     */
    private static final String IS_INCREMENTAL = "hoodie.datasource.jdbc.incremental.pull";

    /**
     * {@value #EXTRA_OPTIONS} used to set any extra options the user specifies for jdbc
     */
    private static final String EXTRA_OPTIONS = "hoodie.datasource.jdbc.extra.options.";

  }
}
