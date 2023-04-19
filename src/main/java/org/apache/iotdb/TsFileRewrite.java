package org.apache.iotdb;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class TsFileRewrite {

  public static void main(String[] args) throws IOException, WriteProcessException {
    new TsFileRewrite(
            "/Users/caozhijia/Desktop/timecho/code/tsfilerewrite/src/main/resources/1681835759143-1-0-0.tsfile",
            "/Users/caozhijia/Desktop/timecho/code/tsfilerewrite/src/main/resources/out.tsfile")
        .parseAndRewriteFile();
  }

  protected TsFileSequenceReader reader;

  protected TsFileIOWriter tsFileIOWriter;
  protected String oldTsFile;
  protected String newTsFile;

  /** Maximum index of plans executed within this TsFile. */
  protected long maxPlanIndex = Long.MIN_VALUE;

  /** Minimum index of plans executed within this TsFile. */
  protected long minPlanIndex = Long.MAX_VALUE;

  public TsFileRewrite(String oldTsFile, String newTsFile) throws IOException {
    this.reader = new TsFileSequenceReader(oldTsFile);
    this.oldTsFile = oldTsFile;
    this.newTsFile = newTsFile;
    initTsFileIOWriter();
  }

  public void initTsFileIOWriter() throws IOException {
    File newFile = FSFactoryProducer.getFSFactory().getFile(newTsFile);
    if (newFile.exists()) {
      System.out.println("delete uncomplated file " + newFile);
      Files.delete(newFile.toPath());
    }
    if (!newFile.createNewFile()) {
      System.out.println("Create new TsFile {} failed because it exists" + newFile);
    }
    this.tsFileIOWriter = new TsFileIOWriter(newFile);
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
            break;
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            MeasurementSchema measurementSchema =
                new MeasurementSchema(
                    header.getMeasurementID(),
                    header.getDataType(),
                    header.getEncodingType(),
                    header.getCompressionType());
            TSDataType dataType = header.getDataType();
            List<PageHeader> pageHeadersInChunk = new ArrayList<>();
            List<ByteBuffer> dataInChunk = new ArrayList<>();
            int dataSize = header.getDataSize();
            while (dataSize > 0) {
              // a new Page
              PageHeader pageHeader =
                  reader.readPageHeader(dataType, header.getChunkType() == MetaMarker.CHUNK_HEADER);
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              pageHeadersInChunk.add(pageHeader);
              dataInChunk.add(pageData);
              dataSize -= pageHeader.getSerializedPageSize();
            }
            //                        deviceId = "root.db.newdevice";
            reWriteChunk(
                deviceId,
                firstChunkInChunkGroup,
                measurementSchema,
                pageHeadersInChunk,
                dataInChunk);
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

            tsFileIOWriter.setMaxPlanIndex(tmpMinPlanIndex);
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
      List<ByteBuffer> pageDataInChunk)
      throws IOException, PageException {
    ChunkWriterImpl chunkWriter = new ChunkWriterImpl(schema);
    for (int i = 0; i < pageDataInChunk.size(); i++) {
      chunkWriter.writePageHeaderAndDataIntoBuff(pageDataInChunk.get(i), pageHeadersInChunk.get(i));
    }
    if (firstChunkInChunkGroup || !tsFileIOWriter.isWritingChunkGroup()) {
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
}
