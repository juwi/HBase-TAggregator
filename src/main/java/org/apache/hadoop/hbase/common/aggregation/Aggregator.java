package org.apache.hadoop.hbase.common.aggregation;

import com.google.protobuf.Message;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by juwi on 11/26/16.
 */
public interface Aggregator {

    <T, S, P extends Message, Q extends Message, R extends Message> Map compute(
            Map results, Cell kv, ColumnInterpreter<T, S, P, Q, R> ci, byte[] columnFamily,
            long timestamp, List<TimeRange> timeRanges) throws IOException;

    TimeseriesAggregateProtos.TimeseriesAggregateResponse wrapForTransport(Map results, ColumnInterpreter ci);
}
