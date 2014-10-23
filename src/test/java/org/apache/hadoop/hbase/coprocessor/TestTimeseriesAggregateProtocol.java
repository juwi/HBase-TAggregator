package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.TimeseriesAggregationClient;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.LongMsg;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestTimeseriesAggregateProtocol {
  protected static Log log = LogFactory.getLog(TestTimeseriesAggregateProtocol.class);

  /**
   * Creating the test infrastructure.
   */
  private static final TableName TEST_TABLE = TableName.valueOf("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final String KEY_FILTER_PATTERN = "00000001111";
  private static String ROW = "testRow";
  private static int TIME_TABLE_BASELINE = (int) ((new GregorianCalendar(2014, 10, 10, 0, 0, 0)
      .getTime().getTime()) / 1000);
  private static final byte[] START_ROW = Bytes.add(ROW.getBytes(),
    Bytes.toBytes(TIME_TABLE_BASELINE));
  private static final byte[] STOP_ROW = Bytes.add(ROW.getBytes(),
    Bytes.toBytes(TIME_TABLE_BASELINE + (3600 * 2)));
  private static final int ROWSIZE = 100;
  private static final int rowSeperator1 = 25;
  private static final int rowSeperator2 = 60;
  private static List<Pair<byte[], Map<byte[], byte[]>>> ROWS = makeN(ROW, ROWSIZE,
    TIME_TABLE_BASELINE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static Configuration conf = util.getConfiguration();

  /**
   * A set up method to start the test cluster. AggregateProtocolImpl is registered and will be
   * loaded during region startup.
   * @throws Exception
   */
  @BeforeClass
  public static void setupBeforeClass() throws Exception {

    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.apache.hadoop.hbase.coprocessor.TimeseriesAggregateImplementation");

    util.startMiniCluster(2);
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY, new byte[][] {
        HConstants.EMPTY_BYTE_ARRAY, ROWS.get(rowSeperator1).getFirst(),
        ROWS.get(rowSeperator2).getFirst() });
    /**
     * The testtable has one CQ which is always populated and one variable CQ for each row rowkey1:
     * CF:CQ CF:CQ1 rowKey2: CF:CQ CF:CQ2
     */
    int time = 0;
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS.get(i).getFirst());
      for (Map.Entry<byte[], byte[]> entry : ROWS.get(i).getSecond().entrySet()) {
        long ts = TIME_TABLE_BASELINE + time + Bytes.toInt(entry.getKey());
        put.add(TEST_FAMILY, entry.getKey(),ts*1000, entry.getValue());
      }
      time += 3600;
      put.setDurability(Durability.SKIP_WAL);
      table.put(put);
    }
    table.close();
  }

  /**
   * Shutting down the cluster
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  /**
   * an infrastructure method to prepare rows for the testtable.
   * @param base
   * @param n
   * @return
   */
  private static List<Pair<byte[], Map<byte[], byte[]>>> makeN(String base, int n, int time) {
    List<Pair<byte[], Map<byte[], byte[]>>> ret =
        new ArrayList<Pair<byte[], Map<byte[], byte[]>>>();
    int interval = 3600 / n;

    for (int i = 0; i < n; i++) {
      Map<byte[], byte[]> innerMap = new LinkedHashMap<byte[], byte[]>();
      byte[] key = Bytes.add(base.getBytes(), Bytes.toBytes(time));
      int cq = 0;
      for (int j = 0; j < n; j++) {
        if (j != 0) cq += interval;
        long value = j;
        innerMap.put(Bytes.toBytes(cq), Bytes.toBytes(value));
      }
      ret.add(new Pair<byte[], Map<byte[], byte[]>>(key, innerMap));
      time += 3600;
    }
    return ret;
  }

  /**
   * ***************Test cases for Maximum *******************
   */

  /**
   * give maxs for a valid range in the table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMaxWithValidRange() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 24l);
    results.put(1415574900000l, 49l);
    results.put(1415575800000l, 74l);
    results.put(1415576700000l, 99l);
    results.put(1415577600000l, 24l);
    results.put(1415578500000l, 49l);
    results.put(1415579400000l, 74l);
    results.put(1415580300000l, 99l);
    results.put(1415581200000l, 24l);

    ConcurrentSkipListMap<Long, Long> maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(results, maximum);
  }

  @Test(timeout = 300000)
  public void testMaxWithValidRange2() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 13, 23, 59, 59).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 99l);
    results.put(1415602800000l, 99l);
    results.put(1415631600000l, 99l);
    results.put(1415660400000l, 99l);
    results.put(1415689200000l, 99l);
    results.put(1415718000000l, 99l);
    results.put(1415746800000l, 99l);
    results.put(1415775600000l, 99l);
    results.put(1415804400000l, 99l);
    results.put(1415833200000l, 99l);
    results.put(1415862000000l, 99l);
    results.put(1415890800000l, 99l);

    ConcurrentSkipListMap<Long, Long> maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(results, maximum);
  }

  /**
   * give maxs for a valid range in the table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMaxWithValidRangeBeginningAtOddTime() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 15, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 4, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415582100000l, 49l);
    results.put(1415583000000l, 74l);
    results.put(1415583900000l, 99l);
    results.put(1415584800000l, 24l);
    results.put(1415585700000l, 49l);
    results.put(1415586600000l, 74l);
    results.put(1415587500000l, 99l);
    results.put(1415588400000l, 24l);

    ConcurrentSkipListMap<Long, Long> maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(results, maximum);
  }

  @Test(timeout = 300000)
  public void testMaxWithRangeBeginningEarlierThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 24l);
    results.put(1415574900000l, 49l);
    results.put(1415575800000l, 74l);
    results.put(1415576700000l, 99l);
    results.put(1415577600000l, 24l);
    results.put(1415578500000l, 49l);
    results.put(1415579400000l, 74l);
    results.put(1415580300000l, 99l);
    results.put(1415581200000l, 24l);

    ConcurrentSkipListMap<Long, Long> maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(results, maximum);
  }

  @Test(timeout = 300000)
  public void testMaxWithRangeLargerThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 15, 0, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415570400000l, 99l);
    results.put(1415599200000l, 99l);
    results.put(1415628000000l, 99l);
    results.put(1415656800000l, 99l);
    results.put(1415685600000l, 99l);
    results.put(1415714400000l, 99l);
    results.put(1415743200000l, 99l);
    results.put(1415772000000l, 99l);
    results.put(1415800800000l, 99l);
    results.put(1415829600000l, 99l);
    results.put(1415858400000l, 99l);
    results.put(1415887200000l, 99l);
    results.put(1415916000000l, 99l);
    ConcurrentSkipListMap<Long, Long> maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(results, maximum);
  }

  @Test(timeout = 300000)
  public void testMaxWithValidRangeUsingScannerRange() throws Throwable {
    long TIME_LIMIT = (new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime());
    TimeseriesAggregationClient aClient = new TimeseriesAggregationClient(conf, 900);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    scan.setTimeRange(TIME_TABLE_BASELINE * 1000l, TIME_LIMIT);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 24l);
    results.put(1415574900000l, 49l);
    results.put(1415575800000l, 74l);
    results.put(1415576700000l, 99l);
    results.put(1415577600000l, 24l);
    results.put(1415578500000l, 49l);
    results.put(1415579400000l, 74l);
    results.put(1415580300000l, 99l);

    ConcurrentSkipListMap<Long, Long> maximum = aClient.max(TEST_TABLE, ci, scan);
    assertEquals(results, maximum);
  }

  /**
   * ***************Test cases for Minimum *******************
   */

  /**
   * give max for the entire table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMinWithValidRange() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 0l);
    results.put(1415574900000l, 25l);
    results.put(1415575800000l, 50l);
    results.put(1415576700000l, 75l);
    results.put(1415577600000l, 0l);
    results.put(1415578500000l, 25l);
    results.put(1415579400000l, 50l);
    results.put(1415580300000l, 75l);
    results.put(1415581200000l, 0l);

    ConcurrentSkipListMap<Long, Long> minimum = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(results, minimum);
  }

  @Test(timeout = 300000)
  public void testMinWithValidRange2() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 13, 23, 59, 59).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 0l);
    results.put(1415602800000l, 0l);
    results.put(1415631600000l, 0l);
    results.put(1415660400000l, 0l);
    results.put(1415689200000l, 0l);
    results.put(1415718000000l, 0l);
    results.put(1415746800000l, 0l);
    results.put(1415775600000l, 0l);
    results.put(1415804400000l, 0l);
    results.put(1415833200000l, 0l);
    results.put(1415862000000l, 0l);
    results.put(1415890800000l, 0l);

    ConcurrentSkipListMap<Long, Long> minimum = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(results, minimum);
  }

  /**
   * give maxs for a valid range in the table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testMinWithValidRangeBeginningAtOddTime() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 15, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 4, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415582100000l, 25l);
    results.put(1415583000000l, 50l);
    results.put(1415583900000l, 75l);
    results.put(1415584800000l, 0l);
    results.put(1415585700000l, 25l);
    results.put(1415586600000l, 50l);
    results.put(1415587500000l, 75l);
    results.put(1415588400000l, 0l);

    ConcurrentSkipListMap<Long, Long> minimum = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(results, minimum);
  }

  @Test(timeout = 300000)
  public void testMinWithRangeBeginningEarlierThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 0l);
    results.put(1415574900000l, 25l);
    results.put(1415575800000l, 50l);
    results.put(1415576700000l, 75l);
    results.put(1415577600000l, 0l);
    results.put(1415578500000l, 25l);
    results.put(1415579400000l, 50l);
    results.put(1415580300000l, 75l);
    results.put(1415581200000l, 0l);

    ConcurrentSkipListMap<Long, Long> minimum = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(results, minimum);
  }

  @Test(timeout = 300000)
  public void testMinWithRangeLargerThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 15, 0, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415570400000l, 0l);
    results.put(1415599200000l, 0l);
    results.put(1415628000000l, 0l);
    results.put(1415656800000l, 0l);
    results.put(1415685600000l, 0l);
    results.put(1415714400000l, 0l);
    results.put(1415743200000l, 0l);
    results.put(1415772000000l, 0l);
    results.put(1415800800000l, 0l);
    results.put(1415829600000l, 0l);
    results.put(1415858400000l, 0l);
    results.put(1415887200000l, 0l);
    results.put(1415916000000l, 0l);
    ConcurrentSkipListMap<Long, Long> minimum = aClient.min(TEST_TABLE, ci, scan);
    assertEquals(results, minimum);
  }

  /**
   * ***************Test cases for Sum *******************
   */

  /**
   * give sum for the entire table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testSumWithValidRange() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 300l);
    results.put(1415574900000l, 925l);
    results.put(1415575800000l, 1550l);
    results.put(1415576700000l, 2175l);
    results.put(1415577600000l, 300l);
    results.put(1415578500000l, 925l);
    results.put(1415579400000l, 1550l);
    results.put(1415580300000l, 2175l);
    results.put(1415581200000l, 300l);

    ConcurrentSkipListMap<Long, Long> sums = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(results, sums);
  }

  @Test(timeout = 300000)
  public void testSumWithValidRange2() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 13, 23, 59, 59).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 39600l);
    results.put(1415602800000l, 39600l);
    results.put(1415631600000l, 39600l);
    results.put(1415660400000l, 39600l);
    results.put(1415689200000l, 39600l);
    results.put(1415718000000l, 39600l);
    results.put(1415746800000l, 39600l);
    results.put(1415775600000l, 39600l);
    results.put(1415804400000l, 39600l);
    results.put(1415833200000l, 39600l);
    results.put(1415862000000l, 39600l);
    results.put(1415890800000l, 39600l);

    ConcurrentSkipListMap<Long, Long> sums = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(results, sums);
  }

  /**
   * give maxs for a valid range in the table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testSumWithValidRangeBeginningAtOddTime() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 15, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 4, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415582100000l, 925l);
    results.put(1415583000000l, 1550l);
    results.put(1415583900000l, 2175l);
    results.put(1415584800000l, 300l);
    results.put(1415585700000l, 925l);
    results.put(1415586600000l, 1550l);
    results.put(1415587500000l, 2175l);
    results.put(1415588400000l, 300l);

    ConcurrentSkipListMap<Long, Long> sums = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(results, sums);
  }

  @Test(timeout = 300000)
  public void testSumWithRangeBeginningEarlierThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 300l);
    results.put(1415574900000l, 925l);
    results.put(1415575800000l, 1550l);
    results.put(1415576700000l, 2175l);
    results.put(1415577600000l, 300l);
    results.put(1415578500000l, 925l);
    results.put(1415579400000l, 1550l);
    results.put(1415580300000l, 2175l);
    results.put(1415581200000l, 300l);

    ConcurrentSkipListMap<Long, Long> sums = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(results, sums);
  }

  @Test(timeout = 300000)
  public void testSumWithRangeLargerThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 15, 0, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Long> results = new ConcurrentSkipListMap<>();
    results.put(1415570400000l, 34650l);
    results.put(1415599200000l, 39600l);
    results.put(1415628000000l, 39600l);
    results.put(1415656800000l, 39600l);
    results.put(1415685600000l, 39600l);
    results.put(1415714400000l, 39600l);
    results.put(1415743200000l, 39600l);
    results.put(1415772000000l, 39600l);
    results.put(1415800800000l, 39600l);
    results.put(1415829600000l, 39600l);
    results.put(1415858400000l, 39600l);
    results.put(1415887200000l, 39600l);
    results.put(1415916000000l, 24750l);
    ConcurrentSkipListMap<Long, Long> sums = aClient.sum(TEST_TABLE, ci, scan);
    assertEquals(results, sums);
  }

  /**
   * ***************Test cases for Avg *******************
   */

  /**
   * give avg for the entire table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testAvgWithValidRange() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Double> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 12.00d);
    results.put(1415574900000l, 37.00d);
    results.put(1415575800000l, 62.00d);
    results.put(1415576700000l, 87.00d);
    results.put(1415577600000l, 12.00d);
    results.put(1415578500000l, 37.00d);
    results.put(1415579400000l, 62.00d);
    results.put(1415580300000l, 87.00d);
    results.put(1415581200000l, 12.00d);

    ConcurrentSkipListMap<Long, Double> avgs = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(results, avgs);
  }

  @Test(timeout = 300000)
  public void testAvgsWithValidRange2() throws Throwable {
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 13, 23, 59, 59).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_TABLE_BASELINE, TIME_LIMIT,
            KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Double> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 49.5d);
    results.put(1415602800000l, 49.5d);
    results.put(1415631600000l, 49.5d);
    results.put(1415660400000l, 49.5d);
    results.put(1415689200000l, 49.5d);
    results.put(1415718000000l, 49.5d);
    results.put(1415746800000l, 49.5d);
    results.put(1415775600000l, 49.5d);
    results.put(1415804400000l, 49.5d);
    results.put(1415833200000l, 49.5d);
    results.put(1415862000000l, 49.5d);
    results.put(1415890800000l, 49.5d);

    ConcurrentSkipListMap<Long, Double> avgs = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(results, avgs);
  }

  /**
   * give maxs for a valid range in the table.
   * @throws Throwable
   */
  @Test(timeout = 300000)
  public void testAvgWithValidRangeBeginningAtOddTime() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 15, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 4, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Double> results = new ConcurrentSkipListMap<>();
    results.put(1415582100000l, 37.00d);
    results.put(1415583000000l, 62.00d);
    results.put(1415583900000l, 87.00d);
    results.put(1415584800000l, 12.00d);
    results.put(1415585700000l, 37.00d);
    results.put(1415586600000l, 62.00d);
    results.put(1415587500000l, 87.00d);
    results.put(1415588400000l, 12.00d);

    ConcurrentSkipListMap<Long, Double> avgs = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(results, avgs);
  }

  @Test(timeout = 300000)
  public void testAvgWithRangeBeginningEarlierThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 10, 2, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 900, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Double> results = new ConcurrentSkipListMap<>();
    results.put(1415574000000l, 12.00d);
    results.put(1415574900000l, 37.00d);
    results.put(1415575800000l, 62.00d);
    results.put(1415576700000l, 87.00d);
    results.put(1415577600000l, 12.00d);
    results.put(1415578500000l, 37.00d);
    results.put(1415579400000l, 62.00d);
    results.put(1415580300000l, 87.00d);
    results.put(1415581200000l, 12.00d);

    ConcurrentSkipListMap<Long, Double> avgs = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(results, avgs);
  }

  @Test(timeout = 300000)
  public void testAvgWithRangeLargerThanTable() throws Throwable {
    int TIME_BASELINE =
        (int) ((new GregorianCalendar(2014, 10, 9, 23, 0, 0).getTime().getTime()) / 1000);
    int TIME_LIMIT =
        (int) ((new GregorianCalendar(2014, 10, 15, 0, 0, 0).getTime().getTime()) / 1000);
    TimeseriesAggregationClient aClient =
        new TimeseriesAggregationClient(conf, 28800, TIME_BASELINE, TIME_LIMIT, KEY_FILTER_PATTERN);
    Scan scan = new Scan();
    scan.addFamily(TEST_FAMILY);
    final ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci =
        new LongColumnInterpreter();
    Map<Long, Double> results = new ConcurrentSkipListMap<>();
    results.put(1415570400000l, 49.5d);
    results.put(1415599200000l, 49.5d);
    results.put(1415628000000l, 49.5d);
    results.put(1415656800000l, 49.5d);
    results.put(1415685600000l, 49.5d);
    results.put(1415714400000l, 49.5d);
    results.put(1415743200000l, 49.5d);
    results.put(1415772000000l, 49.5d);
    results.put(1415800800000l, 49.5d);
    results.put(1415829600000l, 49.5d);
    results.put(1415858400000l, 49.5d);
    results.put(1415887200000l, 49.5d);
    results.put(1415916000000l, 49.5d);
    ConcurrentSkipListMap<Long, Double> avgs = aClient.avg(TEST_TABLE, ci, scan);
    assertEquals(results, avgs);
  }
}