package org.apache.iotdb;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TsFileRewrite {

  public static void main(String[] args)
      throws IOException, WriteProcessException, InterruptedException {
    System.out.println(TsFileRewrite.class.getResource("/"));
    System.out.println(TsFileRewrite.class.getResource(""));
    TsFileRewrite tsFileRewrite =
        new TsFileRewrite(
            new File(
                "/Users/caozhijia/Desktop/timecho/code/tsfilerewrite/src/main/resources/1680079253926-7-0-25.tsfile"),
            new File(
                "/Users/caozhijia/Desktop/timecho/code/tsfilerewrite/src/main/resources/out1.tsfile"));
    tsFileRewrite.parseAndRewriteFile();
    System.out.println(tsFileRewrite.groupCount);
  }

  protected static List<String> strings =
      Arrays.asList(
          "root.Baoshan.I.",
          "root.Baoshan.S.",
          "root.Baoshan.F.",
          "root.Baoshan.R.",
          "root.Baoshan.P.");

  protected TsFileSequenceReader reader;

  protected TsFileIOWriter tsFileIOWriter;
  protected File oldTsFile;
  protected File newTsFile;
  protected Decoder defaultTimeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);
  /** Maximum index of plans executed within this TsFile. */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  protected long minPlanIndex = Long.MAX_VALUE;

  protected Decoder valueDecoder;

  protected int groupCount = 0;

  public TsFileRewrite(File oldTsFile, File newTsFile) throws IOException {
    this.reader = new TsFileSequenceReader(oldTsFile.getAbsolutePath());
    this.oldTsFile = oldTsFile;
    this.newTsFile = newTsFile;
    initTsFileIOWriter();
  }

  public void initTsFileIOWriter() throws IOException {
    if (newTsFile.exists()) {
      System.out.println("delete uncomplated file " + newTsFile);
      Files.delete(newTsFile.toPath());
    }
    if (!newTsFile.createNewFile()) {
      System.out.println("Create new TsFile {} failed because it exists" + newTsFile);
    }
    this.tsFileIOWriter = new TsFileIOWriter(newTsFile);
  }

  public void parseAndRewriteFile() throws IOException, WriteProcessException {
    int headerLength = TSFileConfig.MAGIC_STRING.getBytes().length;
    reader.position(headerLength);

    if (reader.readMarker() != 3) {
      throw new WriteProcessException(
          "The version of this tsfile is too low, please upgrade it to the version 3.");
    }
    // start to scan chunks and chunkGroups
    byte marker;
    String deviceId = null;
    boolean firstChunkInChunkGroup = true;

    try {
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            deviceId = chunkGroupHeader.getDeviceID();
            firstChunkInChunkGroup = true;
            endChunkGroup();

            if (strings.contains(deviceId.substring(0, 15))) {
              deviceId = PathUtil.transformPath(deviceId, 3);
            } else {
              deviceId = PathUtil.transformPath(deviceId, 2);
            }
            break;
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            TSDataType dataType = header.getDataType();
            List<PageHeader> pageHeadersInChunk = new ArrayList<>();
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            List<Boolean> needToDecodeInfo = new ArrayList<>();

            int dataSize = header.getDataSize();
            while (dataSize > 0) {


              // a new Page
              PageHeader pageHeader =
                  reader.readPageHeader(dataType, header.getChunkType() == MetaMarker.CHUNK_HEADER);
              boolean needToDecode = pageHeader.getStatistics() == null;
              needToDecodeInfo.add(needToDecode);

              ByteBuffer pageData = needToDecode ? reader.readPage(pageHeader, header.getCompressionType()) : reader.readCompressedPage(pageHeader);
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
              dataSize -= pageHeader.getSerializedPageSize();
            }

            String measurementID = header.getMeasurementID();
            measurementID = measurementID.replace("\"", "`");
            PartialPath partialPath = new PartialPath(deviceId, measurementID);
            MeasurementSchema measurementSchema =
                new MeasurementSchema(
                    partialPath.getMeasurement(),
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType());

            reWriteChunk(
                partialPath.getDevice(),
                firstChunkInChunkGroup,
                measurementSchema,
                pageHeadersInChunk,
                dataInChunk,needToDecodeInfo);
            firstChunkInChunkGroup = false;
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            // write plan indices for ending memtable
            long tmpMinPlanIndex = reader.getMinPlanIndex();
            if (tmpMinPlanIndex < minPlanIndex) {
              minPlanIndex = tmpMinPlanIndex;
            }

            long tmpMaxPlanIndex = reader.getMaxPlanIndex();
            if (tmpMaxPlanIndex < maxPlanIndex) {
              maxPlanIndex = tmpMaxPlanIndex;
            }

            tsFileIOWriter.setMinPlanIndex(tmpMinPlanIndex);
            tsFileIOWriter.setMaxPlanIndex(tmpMaxPlanIndex);
            tsFileIOWriter.writePlanIndices();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      endChunkGroup();
      tsFileIOWriter.endFile();

    } catch (IOException | PageException e2) {
      throw new IOException(
          "TsFile rewrite process cannot proceed at position "
              + reader.position()
              + "because: "
              + e2.getMessage());
    } catch (IllegalPathException e) {
      throw new RuntimeException(e);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  protected void reWriteChunk(
      String deviceId,
      boolean firstChunkInChunkGroup,
      MeasurementSchema schema,
      List<PageHeader> pageHeadersInChunk,
      List<ByteBuffer> pageDataInChunk,List<Boolean> needToDecodeInfo)
      throws IOException, PageException {
    ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
    valueDecoder = Decoder.getDecoderByType(schema.getEncodingType(), schema.getType());
    for (int i = 0; i < pageDataInChunk.size(); i++) {
      if (Boolean.TRUE.equals(needToDecodeInfo.get(i))) {
        decodeAndWritePage(deviceId, schema, pageDataInChunk.get(i), chunkWriter);
      }else {
        chunkWriter.writePageHeaderAndDataIntoBuff(pageDataInChunk.get(i), pageHeadersInChunk.get(i));
      }
    }
    if (firstChunkInChunkGroup || !tsFileIOWriter.isWritingChunkGroup()) {
      groupCount++;
      tsFileIOWriter.startChunkGroup(deviceId);
    }
    // write chunks to their own upgraded tsFiles
    chunkWriter.writeToFileWriter(tsFileIOWriter);
  }

  protected void endChunkGroup() throws IOException {
    tsFileIOWriter.endChunkGroup();
  }

  public void close() throws IOException {
    this.reader.close();
  }

  protected void decodeAndWritePage(
      String deviceId, MeasurementSchema schema, ByteBuffer pageData, ChunkWriterImpl chunkWriter)
      throws IOException {
    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, schema.getType(), valueDecoder, defaultTimeDecoder, null);
    // read delete time range from old modification file
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    rewritePageIntoFiles(batchData, schema, chunkWriter);
  }

  protected void rewritePageIntoFiles(
      BatchData batchData, MeasurementSchema schema, ChunkWriterImpl chunkWriter) {

    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      Object value = batchData.currentValue();
      switch (schema.getType()) {
        case INT32:
          chunkWriter.write(time, (int) value);
          break;
        case INT64:
          chunkWriter.write(time, (long) value);
          break;
        case FLOAT:
          chunkWriter.write(time, (float) value);
          break;
        case DOUBLE:
          chunkWriter.write(time, (double) value);
          break;
        case BOOLEAN:
          chunkWriter.write(time, (boolean) value);
          break;
        case TEXT:
          chunkWriter.write(time, (Binary) value);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", schema.getType()));
      }
      batchData.next();
    }
    chunkWriter.sealCurrentPage();
  }
}
