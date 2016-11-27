package org.apache.hadoop.hbase.common;

import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.common.aggregation.Aggregator;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.TimeseriesAggregateProtos;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by juwi on 11/26/16.
 */
public class AggregationBuilder<T, S, P extends Message, Q extends Message, R extends Message> {

    private static final Log log = LogFactory.getLog(AggregationBuilder.class);

    private ColumnInterpreter<T, S, P, Q, R> columnInterpreter;

    private boolean isScanTimeRanged;
    private String keySchema;
    private InternalScanner scanner;
    private List<TimeRange> timeRanges;
    private byte[] columnFamily;
    private Aggregator aggregator;

    Map results = new HashMap();

    public AggregationBuilder setColumnInterpreter(ColumnInterpreter ci) {
        this.columnInterpreter = ci;
        return this;
    }

    public AggregationBuilder isScanTimeRanged(boolean b) {
        this.isScanTimeRanged = b;
        return this;
    }

    public AggregationBuilder setKeySchema(String ks) {
        this.keySchema = ks;
        return this;
    }

    public AggregationBuilder setInternalScanner(InternalScanner s) {
        this.scanner = s;
        return this;
    }

    public AggregationBuilder setTimeRanges(List<TimeRange> tr) {
        this.timeRanges = tr;
        return this;
    }

    public AggregationBuilder setColumnFamily(byte[] colFam) {
        this.columnFamily = colFam;
        return this;
    }

    public AggregationBuilder setAggregator(Aggregator a) {
        this.aggregator = a;
        return this;
    }

    public AggregationBuilder aggregate() throws IOException {
        List<Cell> cellResults = new ArrayList<Cell>();

        boolean hasMoreRows;
        do {
            cellResults.clear();
            hasMoreRows = scanner.next(cellResults);
            for (Cell kv : cellResults) {
                long timestamp;
                if (isScanTimeRanged) timestamp = kv.getTimestamp();
                else timestamp =
                        getMillisTimestampFromOffset(getTimestampFromRowKeyAsMillis(kv, keySchema),
                                Bytes.toInt(CellUtil.cloneQualifier(kv)));
                results = aggregator.compute(results, kv, columnInterpreter, columnFamily, timestamp, timeRanges);
            }
        } while (hasMoreRows);
        return this;
    }

    public TimeseriesAggregateProtos.TimeseriesAggregateResponse build() {
        return results.isEmpty() ? null: aggregator.wrapForTransport(results, columnInterpreter);
    }

    public long getMillisTimestampFromOffset(long currentTimeStamp, int offset) {
        long offsetMicro = (long) offset * 1000l;
        return currentTimeStamp + offsetMicro;
    }

    private int getTimestampFromRowKeyAsSeconds(Cell kv, String keySchema) {
        byte[] rowKey = CellUtil.cloneRow(kv);
        if (keySchema.length() != rowKey.length) {
            log.error("Row Key:" + rowKey + ", Pattern: " + keySchema);
            log.error("Timestamp Filter Pattern and Row Key length do not match. Don't know how to handle this.");
            return 0;
        }
        byte[] ts = new byte[4];
        int j = 0;
        for (int i = keySchema.indexOf("1"); i <= keySchema.lastIndexOf("1"); i++, j++) {
            ts[j] = rowKey[i];
        }
        return Bytes.toInt(ts);
    }

    public long getTimestampFromRowKeyAsMillis(Cell kv, String keySchema) {
        long ts = getTimestampFromRowKeyAsSeconds(kv, keySchema);
        return ts * 1000l;
    }
}
