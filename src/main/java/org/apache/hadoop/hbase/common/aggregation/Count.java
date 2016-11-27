package org.apache.hadoop.hbase.common.aggregation;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by juwi on 11/27/16.
 */
public class Count<T, S, P extends Message, Q extends Message, R extends Message> implements Aggregator {
    @Override
    public <T, S, P extends Message, Q extends Message, R extends Message> Map compute(
            Map results, Cell kv, ColumnInterpreter<T, S, P, Q, R> ci, byte[] columnFamily, long timestamp,
            List<TimeRange> timeRanges)
            throws IOException {
        Map<Long, Long> counts = results;
        long count;
        for (TimeRange t : timeRanges) {
            if (t.withinTimeRange(timestamp)) {
                long minTimestamp = t.getMin();
                if (counts.containsKey(minTimestamp)) {
                    count = counts.get(minTimestamp);
                    count++;
                } else count = 1L;
                counts.put(minTimestamp, count);
            }
        }
        return counts;
    }

    @Override
    public TimeseriesAggregateProtos.TimeseriesAggregateResponse wrapForTransport(Map results, ColumnInterpreter ci) {
        Map<Long, Long> counts = results;
        TimeseriesAggregateProtos.TimeseriesAggregateResponse.Builder responseBuilder =
                TimeseriesAggregateProtos.TimeseriesAggregateResponse.newBuilder();

        for (Map.Entry<Long, Long> entry : counts.entrySet()) {
            TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.Builder valueBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseEntry.newBuilder();
            TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.Builder mapElementBuilder =
                    TimeseriesAggregateProtos.TimeseriesAggregateResponseMapEntry.newBuilder();

            valueBuilder.addFirstPart(ByteString.copyFrom(Bytes.toBytes(entry.getValue())));

            mapElementBuilder.setKey(entry.getKey());
            mapElementBuilder.setValue(valueBuilder.build());

            responseBuilder.addEntry(mapElementBuilder.build());
        }
        return responseBuilder.build();
    }
}
