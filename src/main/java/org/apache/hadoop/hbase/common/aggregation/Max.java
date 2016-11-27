package org.apache.hadoop.hbase.common.aggregation;

import com.google.protobuf.Message;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by juwi on 11/26/16.
 */
public class Max<T, S, P extends Message, Q extends Message, R extends Message> implements Aggregator {
    @Override
    public <T, S, P extends Message, Q extends Message, R extends Message> Map<Long, T> compute(
            Map results, Cell kv, ColumnInterpreter<T, S, P, Q, R> ci, byte[] columnFamily, long timestamp,
            List<TimeRange> timeRanges) throws IOException {
        Map<Long, T> maximums = results;
        ColumnInterpreter<T, S, P, Q, R> columnInterpreter = ci;
        T temp;
        T max;
        for (TimeRange t : timeRanges) {
            if (t.withinTimeRange(timestamp)) {
                long minTimestamp = t.getMin();
                if (maximums.containsKey(minTimestamp)) {
                    max = maximums.get(minTimestamp);
                } else max = null;
                temp = columnInterpreter.getValue(columnFamily, CellUtil.cloneQualifier(kv), kv);
                max = (max == null || (temp != null && ci.compare(temp, max) > 0)) ? temp : max;
                maximums.put(minTimestamp, max);
            }
        }
        return maximums;
    }

    @Override
    public TimeseriesAggregateProtos.TimeseriesAggregateResponse wrapForTransport(Map results, ColumnInterpreter ci) {
        Map<Long, T> maximums = results;
        TimeseriesAggregateProtos.TimeseriesAggregateResponse.Builder responseBuilder =
                TimeseriesAggregateProtos.TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, T> entry : maximums.entrySet()) {
            TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.Builder valueBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.newBuilder();
            TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.newBuilder();

            valueBuilder.addFirstPart(ci.getProtoForCellType(entry.getValue()).toByteString());

            mapElementBuilder.setKey(entry.getKey());
            mapElementBuilder.setValue(valueBuilder.build());

            responseBuilder.addEntry(mapElementBuilder.build());
        }
        return responseBuilder.build();
    }
}
