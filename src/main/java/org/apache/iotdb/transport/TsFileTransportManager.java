package org.apache.iotdb.transport;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TsFileTransportManager {

  private ExecutorService executorService;

  private List<TsFileTransporter> transporterList;

  public TsFileTransportManager() {}

  public void init(int concurrentNum, String host, int port, String user, String pwd) {
    executorService = Executors.newFixedThreadPool(concurrentNum);
    transporterList = new ArrayList<>(concurrentNum);
    for (int i = 0; i < concurrentNum; i++) {
      TsFileTransporter tsFileTransporter = new TsFileTransporter(host, port, user, pwd);
      tsFileTransporter.init();
      transporterList.add(tsFileTransporter);
    }
  }

  public void clear() {
    if (executorService != null) {
      executorService.shutdown();
      while (true) {
        if (executorService.isTerminated()) {
          break;
        }
      }
    }
    for (TsFileTransporter tsFileTransporter : transporterList) {
      tsFileTransporter.clear();
    }
  }

  public Future<?> submitTsFileTransportTask(
      String sourceDir, String sourceTsFileName, String remoteDirPath) {
    return executorService.submit(
        () -> transportTsFile(sourceDir, sourceTsFileName, remoteDirPath));
  }

  private void transportTsFile(String sourceDir, String sourceTsFileName, String remoteDirPath) {
    TsFileTransporter tsFileTransporter = borrowTsFileTransporter();
    while (tsFileTransporter == null) {
      tsFileTransporter = borrowTsFileTransporter();
    }
    tsFileTransporter.upLoadFile(remoteDirPath, sourceDir + File.separator + sourceTsFileName);
    returnTsFileTransporter(tsFileTransporter);
    File sourceTsFile = new File(sourceDir + File.separator + sourceTsFileName);
    sourceTsFile.delete();
  }

  private synchronized TsFileTransporter borrowTsFileTransporter() {
    for (TsFileTransporter tsFileTransporter : transporterList) {
      if (!tsFileTransporter.isUsed()) {
        tsFileTransporter.setUsed(true);
        return tsFileTransporter;
      }
    }
    return null;
  }

  private synchronized void returnTsFileTransporter(TsFileTransporter tsFileTransporter) {
    tsFileTransporter.setUsed(false);
  }
}
