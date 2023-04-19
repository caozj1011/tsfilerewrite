package org.apache.iotdb;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsfileRewriteTool {

    public static final String ENCODING = "PLAIN";
    public static final String COMPRESSOR = "SNAPPY";
    public static final String TSFILE_DIR = "";
    public static final int THREAD_COUNT = 1;

    public static final boolean IS_ALIGNED = false;

    public static void main(String[] args) throws IOException {
        try(TsFileSequenceReader tsFileSequenceReader = new TsFileSequenceReader("/Users/caozhijia/Desktop/timecho/code/tsfilerewrite/src/main/resources/1681832331466-1-0-0.tsfile");) {
            TsFileReader reader = new TsFileReader(tsFileSequenceReader);
            Map<String, List<String>> deviceMeasurementsMap = tsFileSequenceReader.getDeviceMeasurementsMap();
            Set<Map.Entry<String, List<String>>> entries = deviceMeasurementsMap.entrySet();
            List<Path> paths = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : entries) {
                String deviceId = entry.getKey();
                List<String> value = entry.getValue();
                for (String measurement : value) {
                    paths.add(new Path(deviceId,measurement,true));
                }
            }
            QueryDataSet dataSet = reader.query(QueryExpression.create(paths,null));



            System.out.println(dataSet);
            writeTsFileFile(dataSet,"./test.tsfile");
        } catch (WriteProcessException e) {
            throw new RuntimeException(e);
        }
    }



    public static void writeTsFileFile(QueryDataSet dataSet, String filePath) throws IOException, WriteProcessException {

        List<TSDataType> columnTypes = dataSet.getDataTypes();
        List<Path> columnNames = dataSet.getPaths();

        File f = FSFactoryProducer.getFSFactory().getFile(filePath);
        if (f.exists()) {
            f.delete();
        }
        try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
            Map<String, List<MeasurementSchema>> schemaMap = new LinkedHashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                Path path = columnNames.get(i);
                String column = path.getFullPath();
                if (!column.startsWith("root.")) {
                    continue;
                }

                TSDataType tsDataType = columnTypes.get(i);
                String deviceId = path.getDevice();
                MeasurementSchema measurementSchema = new MeasurementSchema(path.getMeasurement(), tsDataType);
                // todo 可否写死
                measurementSchema.setEncoding(TSEncoding.valueOf(ENCODING).serialize());
                // todo 可否写死
                measurementSchema.setCompressor(CompressionType.valueOf(COMPRESSOR).serialize());
                schemaMap.computeIfAbsent(deviceId, key -> new ArrayList<>()).add(measurementSchema);
            }
            List<Tablet> tabletList = new ArrayList<>();
            for (String deviceId : schemaMap.keySet()) {
                List<MeasurementSchema> schemaList = schemaMap.get(deviceId);
                Tablet tablet = new Tablet(deviceId, schemaList);
                tablet.initBitMaps();
                Path path = new Path(tablet.deviceId);
                // todo 可否写死
                if (IS_ALIGNED) {
                    tsFileWriter.registerAlignedTimeseries(path, schemaList);
                } else {
                    tsFileWriter.registerTimeseries(path, schemaList);
                }
                tabletList.add(tablet);
            }
            if (tabletList.isEmpty()) {
                System.out.println("!!!Warning:Tablet is empty,no data can be exported.");
                System.exit(-1);
            }
            while (dataSet.hasNext()) {
                RowRecord rowRecord = dataSet.next();
                List<Field> fields = rowRecord.getFields();
                for (int i = 0; i < fields.size(); ) {
                    for (Tablet tablet : tabletList) {
                        int rowIndex = tablet.rowSize++;
                        tablet.addTimestamp(rowIndex, rowRecord.getTimestamp());
                        List<MeasurementSchema> schemas = tablet.getSchemas();
                        for (int j = 0; j < schemas.size(); j++) {
                            MeasurementSchema measurementSchema = schemas.get(j);
                            Field field = fields.get(i);
                            if (field == null) {
                                tablet.bitMaps[j].mark(rowIndex);
                            }else {
                                Object value = field.getObjectValue(measurementSchema.getType());
                                tablet.addValue(measurementSchema.getMeasurementId(), rowIndex, value);
                            }

                            i++;
                        }
                        if (tablet.rowSize == tablet.getMaxRowNumber()) {
                            writeToTsfile(tsFileWriter, tablet);
                            tablet.initBitMaps();
                            tablet.reset();
                        }
                    }
                }
            }
            for (Tablet tablet : tabletList) {
                if (tablet.rowSize != 0) {
                    writeToTsfile(tsFileWriter, tablet);
                }
            }
            tsFileWriter.flushAllChunkGroups();
        }
    }

    private static void writeToTsfile(TsFileWriter tsFileWriter, Tablet tablet) throws IOException, WriteProcessException {
        // todo 可否写死
        if (IS_ALIGNED) {
            tsFileWriter.writeAligned(tablet);
        } else {
            tsFileWriter.write(tablet);
        }
    }


    public boolean isMatch(String s, String p) {
        int m = s.length();
        int n = p.length();
        boolean[][] dp = new boolean[m + 1][n + 1];
        dp[0][0] = true;
        for (int i = 1; i <= n; ++i) {
            if (p.charAt(i - 1) == '*') {
                dp[0][i] = true;
            } else {
                break;
            }
        }
        for (int i = 1; i <= m; ++i) {
            for (int j = 1; j <= n; ++j) {
                if (p.charAt(j - 1) == '*') {
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                } else if (p.charAt(j - 1) == '?' || s.charAt(i - 1) == p.charAt(j - 1)) {
                    dp[i][j] = dp[i - 1][j - 1];
                }
            }
        }
        return dp[m][n];
    }

}
