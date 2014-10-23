package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesRange;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos.TimeseriesAggregateService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * This client class is for invoking the aggregate functions deployed on the Region Server side via
 * the TimeseriesAggregateService. This class will implement the supporting functionality for
 * summing/processing the individual results obtained from the TimeseriesAggregateService for each
 * region.
 * <p>
 * This will serve as the client side handler for invoking the aggregate functions.
 * <ul>
 * For all aggregate functions,
 * <li>start row < end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are provided, an IOException
 * will be thrown. An optional column qualifier can also be defined.
 * <li>For methods to find maximum, minimum, sum, rowcount, it returns the parameter type. For
 * average and std, it returns a double value. For row count, it returns a long value. <T> Cell
 * value data type <S> Promoted data type
 * <P>
 * PB message that is used to transport initializer specific bytes
 * <Q>PB message that is used to transport Cell (<T>) instance <R> PB message that is used to
 * transport Promoted (<S>) instance
 */
@InterfaceAudience.Private
public class TimeseriesAggregationClient {

  private static final Log log = LogFactory.getLog(TimeseriesAggregationClient.class);
  private Configuration conf;
  private int intervalSeconds;
  private Integer timestampSecondsMin, timestampSecondsMax;
  private String keyFilterPattern;

  /**
   * Constructor with Conf object
   * @param cfg
   */
  public TimeseriesAggregationClient(Configuration cfg, int intervalSeconds) {
    this.conf = cfg;
    this.intervalSeconds = intervalSeconds;
  }

  /**
   * Constructor with Conf object
   * @param cfg
   */
  public TimeseriesAggregationClient(Configuration cfg, int intervalSeconds,
      int timestampSecondsMin, int timestampSecondsMax, String keyFilterPattern) {
    this.conf = cfg;
    this.intervalSeconds = intervalSeconds;
    this.timestampSecondsMin = timestampSecondsMin;
    this.timestampSecondsMax = timestampSecondsMax;
    this.keyFilterPattern = keyFilterPattern;
  }

  /**
   * It gives the maximum value of a column for a given column family for the given range. In case
   * qualifier is null, a max of all values for the given family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return max val ConcurrentSkipListMap<Long, R> (Will come as proto from region needs to be
   *         passed out as ConcurrentSkipListMap)
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &
   *           propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, R> max(final TableName tableName,
          final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    Table table = null;
    try {
      table = new HTable(conf, tableName);
      return max(table, ci, scan);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * It gives the maximum value of a column for a given column family for the given range. In case
   * qualifier is null, a max of all values for the given family is returned.
   * @param table
   * @param ci
   * @param scan
   * @return max val ConcurrentSkipListMap<Long, R> (Will come as proto from region needs to be
   *         passed out as ConcurrentSkipListMap)
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &
   *           propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, R> max(final Table table,
          final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final TimeseriesAggregateRequest requestArg =
        validateArgAndGetPB(scan, ci, false, intervalSeconds, timestampSecondsMin,
          timestampSecondsMax, keyFilterPattern);
    class MaxCallBack implements Batch.Callback<TimeseriesAggregateResponse> {
      ConcurrentSkipListMap<Long, R> max = new ConcurrentSkipListMap<>();

      ConcurrentSkipListMap<Long, R> getMax() {
        return max;
      }

      @Override
      public synchronized void
          update(byte[] region, byte[] row, TimeseriesAggregateResponse result) {
        List<TimeseriesAggregateResponseMapEntry> results =
            ((TimeseriesAggregateResponse) result).getEntryList();
        for (TimeseriesAggregateResponseMapEntry entry : results) {
          R candidate;
          if (entry.getValue().getFirstPartCount() > 0) {
            ByteString b = entry.getValue().getFirstPart(0);
            Q q = null;
            try {
              q = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 3, b);
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            candidate = ci.getCellValueFromProto(q);
            if (null != q) {
              if (max.containsKey(entry.getKey())) {
                R current = max.get(entry.getKey());
                max.put(entry.getKey(), (current == null || (candidate != null && ci.compare(
                  current, candidate) < 0)) ? candidate : current);
              } else {
                max.put(entry.getKey(), ci.getCellValueFromProto(q));
              }
            }
          }
        }
      }
    }

    MaxCallBack aMaxCallBack = new MaxCallBack();
    table.coprocessorService(TimeseriesAggregateService.class, scan.getStartRow(),
      scan.getStopRow(), new Batch.Call<TimeseriesAggregateService, TimeseriesAggregateResponse>() {
        @Override
        public TimeseriesAggregateResponse call(TimeseriesAggregateService instance)
            throws IOException {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<TimeseriesAggregateResponse> rpcCallback =
              new BlockingRpcCallback<TimeseriesAggregateResponse>();
          instance.getMax(controller, requestArg, rpcCallback);
          TimeseriesAggregateResponse response = rpcCallback.get();
          if (controller.failedOnException()) {
            throw controller.getFailedOn();
          }
          return response;
        }
      }, aMaxCallBack);
    return aMaxCallBack.getMax();
  }

  /**
   * It gives the minimum value of a column for a given column family for the given range. In case
   * qualifier is null, a min of all values for the given family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return min val ConcurrentSkipListMap<Long, R> (Will come as proto from region needs to be
   *         passed out as ConcurrentSkipListMap)
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &
   *           propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, R> min(final TableName tableName,
          final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    Table table = null;
    try {
      table = new HTable(conf, tableName);
      return min(table, ci, scan);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * It gives the minimum value of a column for a given column family for the given range. In case
   * qualifier is null, a min of all values for the given family is returned.
   * @param table
   * @param ci
   * @param scan
   * @return min val ConcurrentSkipListMap<Long, R> (Will come as proto from region needs to be
   *         passed out as ConcurrentSkipListMap)
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &
   *           propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, R> min(final Table table,
          final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final TimeseriesAggregateRequest requestArg =
        validateArgAndGetPB(scan, ci, false, intervalSeconds, timestampSecondsMin,
          timestampSecondsMax, keyFilterPattern);
    class MinCallBack implements Batch.Callback<TimeseriesAggregateResponse> {
      ConcurrentSkipListMap<Long, R> min = new ConcurrentSkipListMap<>();

      ConcurrentSkipListMap<Long, R> getMin() {
        return min;
      }

      @Override
      public synchronized void
          update(byte[] region, byte[] row, TimeseriesAggregateResponse result) {
        List<TimeseriesAggregateResponseMapEntry> results =
            ((TimeseriesAggregateResponse) result).getEntryList();
        for (TimeseriesAggregateResponseMapEntry entry : results) {
          R candidate;
          if (entry.getValue().getFirstPartCount() > 0) {
            ByteString b = entry.getValue().getFirstPart(0);
            Q q = null;
            try {
              q = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 3, b);
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            candidate = ci.getCellValueFromProto(q);
            if (null != q) {
              if (min.containsKey(entry.getKey())) {
                R current = min.get(entry.getKey());
                min.put(entry.getKey(), (current == null || (candidate != null && ci.compare(
                  current, candidate) < 0)) ? current : candidate);
              } else {
                min.put(entry.getKey(), ci.getCellValueFromProto(q));
              }
            }
          }
        }
      }
    }

    MinCallBack aMinCallBack = new MinCallBack();
    table.coprocessorService(TimeseriesAggregateService.class, scan.getStartRow(),
      scan.getStopRow(), new Batch.Call<TimeseriesAggregateService, TimeseriesAggregateResponse>() {
        @Override
        public TimeseriesAggregateResponse call(TimeseriesAggregateService instance)
            throws IOException {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<TimeseriesAggregateResponse> rpcCallback =
              new BlockingRpcCallback<TimeseriesAggregateResponse>();
          instance.getMin(controller, requestArg, rpcCallback);
          TimeseriesAggregateResponse response = rpcCallback.get();
          if (controller.failedOnException()) {
            throw controller.getFailedOn();
          }
            return response;
        }
      }, aMinCallBack);
    return aMinCallBack.getMin();
  }

  /**
   * It gives the sum value of a column for a given column family for the given range. In case
   * qualifier is null, a sum of all values for the given family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return sum val ConcurrentSkipListMap<Long, R> (Will come as proto from region needs to be
   *         passed out as ConcurrentSkipListMap)
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &
   *           propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, S> sum(final TableName tableName,
          final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    Table table = null;
    try {
      table = new HTable(conf, tableName);
      return sum(table, ci, scan);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * It gives the sum value of a column for a given column family for the given range. In case
   * qualifier is null, a sum of all values for the given family is returned.
   * @param table
   * @param ci
   * @param scan
   * @return sum val ConcurrentSkipListMap<Long, R> (Will come as proto from region needs to be
   *         passed out as ConcurrentSkipListMap)
   * @throws Throwable The caller is supposed to handle the exception as they are thrown &
   *           propagated to it.
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, S> sum(final Table table,
          final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final TimeseriesAggregateRequest requestArg =
        validateArgAndGetPB(scan, ci, false, intervalSeconds, timestampSecondsMin,
          timestampSecondsMax, keyFilterPattern);
    class SumCallBack implements Batch.Callback<TimeseriesAggregateResponse> {
      ConcurrentSkipListMap<Long, S> sum = new ConcurrentSkipListMap<>();

      ConcurrentSkipListMap<Long, S> getMax() {
        return sum;
      }

      @Override
      public synchronized void
          update(byte[] region, byte[] row, TimeseriesAggregateResponse result) {
        List<TimeseriesAggregateResponseMapEntry> results =
            ((TimeseriesAggregateResponse) result).getEntryList();
        for (TimeseriesAggregateResponseMapEntry entry : results) {
          S candidate;
          if (entry.getValue().getFirstPartCount() == 0) {
            if (!sum.containsKey(entry.getKey())) {
              sum.put(entry.getKey(), null);
            }
          } else {
            ByteString b = entry.getValue().getFirstPart(0);
            T t = null;
            try {
              t = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 4, b);
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            candidate = ci.getPromotedValueFromProto(t);
            if (null != t) {
              if (sum.containsKey(entry.getKey())) {
                S current = sum.get(entry.getKey());
                sum.put(entry.getKey(), (ci.add(current, candidate)));
              } else {
                if (entry.getValue().getFirstPartCount() == 0) {
                  sum.put(entry.getKey(), null);
                } else {
                  sum.put(entry.getKey(), candidate);
                }
              }
            }
          }
        }
      }
    }

    SumCallBack aSumCallBack = new SumCallBack();
    table.coprocessorService(TimeseriesAggregateService.class, scan.getStartRow(),
      scan.getStopRow(), new Batch.Call<TimeseriesAggregateService, TimeseriesAggregateResponse>() {
        @Override
        public TimeseriesAggregateResponse call(TimeseriesAggregateService instance)
            throws IOException {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<TimeseriesAggregateResponse> rpcCallback =
              new BlockingRpcCallback<TimeseriesAggregateResponse>();
          instance.getSum(controller, requestArg, rpcCallback);
          TimeseriesAggregateResponse response = rpcCallback.get();
          if (controller.failedOnException()) {
            throw controller.getFailedOn();
          }
          // if (response.getEntryCount() > 0) {
          return response;
          // }
          // return null;
        }
      }, aSumCallBack);
    return aSumCallBack.getMax();
  }

  /**
   * It computes average while fetching sum and row count from all the corresponding regions.
   * Approach is to compute a global sum of region level sum and rowcount and then compute the
   * average.
   * @param table
   * @param scan
   * @throws Throwable
   */
  private <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, Pair<S, Long>> getAvgArgs(final Table table,
          final ColumnInterpreter<R, S, P, Q, T> ci, final Scan scan) throws Throwable {
    final TimeseriesAggregateRequest requestArg =
        validateArgAndGetPB(scan, ci, false, intervalSeconds, timestampSecondsMin,
          timestampSecondsMax, keyFilterPattern);
    class AvgCallBack implements Batch.Callback<TimeseriesAggregateResponse> {
      ConcurrentSkipListMap<Long, Pair<S, Long>> averages = new ConcurrentSkipListMap<>();

      public synchronized ConcurrentSkipListMap<Long, Pair<S, Long>> getAvgArgs() {
        return averages;
      }

      @Override
      public synchronized void
          update(byte[] region, byte[] row, TimeseriesAggregateResponse result) {
        List<TimeseriesAggregateResponseMapEntry> results = result.getEntryList();
        for (TimeseriesAggregateResponseMapEntry entry : results) {

          if (entry.getValue().getFirstPartCount() == 0) {
            if (!averages.containsKey(entry.getKey())) {
              averages.put(entry.getKey(), new Pair<S, Long>(null, 0L));
            }
          } else {

            ByteString b = entry.getValue().getFirstPart(0);
            T t = null;
            try {
              t = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 4, b);
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            S s = ci.getPromotedValueFromProto(t);

            ByteBuffer bb =
                ByteBuffer.allocate(8).put(getBytesFromResponse(entry.getValue().getSecondPart()));
            bb.rewind();

            if (averages.containsKey(entry.getKey())) {
              S sum = averages.get(entry.getKey()).getFirst();
              Long rowCount = averages.get(entry.getKey()).getSecond();
              averages.put(entry.getKey(),
                new Pair<S, Long>(ci.add(sum, s), rowCount + bb.getLong()));
            } else {
              averages.put(entry.getKey(), new Pair<S, Long>(s, bb.getLong()));
            }
          }
        }
      }
    }
    AvgCallBack avgCallBack = new AvgCallBack();
    table.coprocessorService(TimeseriesAggregateService.class, scan.getStartRow(),
      scan.getStopRow(), new Batch.Call<TimeseriesAggregateService, TimeseriesAggregateResponse>() {
        @Override
        public TimeseriesAggregateResponse call(TimeseriesAggregateService instance)
            throws IOException {
          ServerRpcController controller = new ServerRpcController();
          BlockingRpcCallback<TimeseriesAggregateResponse> rpcCallback =
              new BlockingRpcCallback<TimeseriesAggregateResponse>();
          instance.getAvg(controller, requestArg, rpcCallback);
          TimeseriesAggregateResponse response = rpcCallback.get();
          if (controller.failedOnException()) {
            throw controller.getFailedOn();
          }
          // if (response.getEntryCount() > 0) {
          return response;
          // }
          // return null;
        }
      }, avgCallBack);
    return avgCallBack.getAvgArgs();
  }

  /**
   * This is the client side interface/handle for calling the average method for a given cf-cq
   * combination. It was necessary to add one more call stack as its return type should be a decimal
   * value, irrespective of what columninterpreter says. So, this methods collects the necessary
   * parameters to compute the average and returs the double value.
   * @param tableName
   * @param ci
   * @param scan
   * @return <R, S>
   * @throws Throwable
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, Double> avg(final TableName tableName,
          final ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) throws Throwable {
    Table table = null;
    try {
      table = new HTable(conf, tableName);
      return avg(table, ci, scan);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  /**
   * This is the client side interface/handle for calling the average method for a given cf-cq
   * combination. It was necessary to add one more call stack as its return type should be a decimal
   * value, irrespective of what columninterpreter says. So, this methods collects the necessary
   * parameters to compute the average and returs the double value.
   * @param table
   * @param ci
   * @param scan
   * @return <R, S>
   * @throws Throwable
   */
  public <R, S, P extends Message, Q extends Message, T extends Message>
      ConcurrentSkipListMap<Long, Double> avg(final Table table,
          final ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) throws Throwable {
    ConcurrentSkipListMap<Long, Pair<S, Long>> p = getAvgArgs(table, ci, scan);
    ConcurrentSkipListMap<Long, Double> avg = new ConcurrentSkipListMap<Long, Double>();
    for (Map.Entry<Long, Pair<S, Long>> entry : p.entrySet()) {
      avg.put(entry.getKey(),
        ci.divideForAvg(entry.getValue().getFirst(), entry.getValue().getSecond()));
    }
    return avg;
  }

  byte[] getBytesFromResponse(ByteString response) {
    ByteBuffer bb = response.asReadOnlyByteBuffer();
    bb.rewind();
    byte[] bytes;
    if (bb.hasArray()) {
      bytes = bb.array();
    } else {
      bytes = response.toByteArray();
    }
    return bytes;
  }

  /*
   * @param scan
   * @param canFamilyBeAbsent whether column family can be absent in familyMap of scan
   */
  private void validateParameters(Scan scan, boolean canFamilyBeAbsent) throws IOException {
    if (scan == null
        || (Bytes.equals(scan.getStartRow(), scan.getStopRow()) && !Bytes.equals(
          scan.getStartRow(), HConstants.EMPTY_START_ROW))
        || ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) && !Bytes.equals(
          scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
      throw new IOException("Agg client Exception: Startrow should be smaller than Stoprow");
    } else if (!canFamilyBeAbsent) {
      if (scan.getFamilyMap().size() != 1) {
        throw new IOException("There must be only one family.");
      }
    }
  }

  <R, S, P extends Message, Q extends Message, T extends Message> TimeseriesAggregateRequest
      validateArgAndGetPB(Scan scan, ColumnInterpreter<R, S, P, Q, T> ci,
          boolean canFamilyBeAbsent, int intervalSeconds, Integer timestampSecondsMin,
          Integer timestampSecondsMax, String keyFilterPattern) throws IOException {
    validateParameters(scan, canFamilyBeAbsent);
    final TimeseriesAggregateRequest.Builder requestBuilder =
        TimeseriesAggregateRequest.newBuilder();
    requestBuilder.setInterpreterClassName(ci.getClass().getCanonicalName());
    P columnInterpreterSpecificData = null;
    if ((columnInterpreterSpecificData = ci.getRequestData()) != null) {
      requestBuilder.setInterpreterSpecificBytes(columnInterpreterSpecificData.toByteString());
    }
    requestBuilder.setScan(ProtobufUtil.toScan(scan));

    requestBuilder.setTimeIntervalSeconds(intervalSeconds);
    if (!(null == timestampSecondsMin)) {
      final TimeseriesRange.Builder rangeBuilder = TimeseriesRange.newBuilder();
      rangeBuilder.setKeyTimestampMin(timestampSecondsMin);
      rangeBuilder.setKeyTimestampMax(timestampSecondsMax);
      rangeBuilder.setKeyTimestampFilterPattern(keyFilterPattern);
      requestBuilder.setRange(rangeBuilder.build());
    }
    return requestBuilder.build();
  }
}
