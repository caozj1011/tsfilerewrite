package org.apache.iotdb.transport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TsFileTransportManager {

  private ExecutorService executorService;

  public TsFileTransportManager() {}

  public void init(int concurrentNum) {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(concurrentNum);
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
  }

  public Future<?> submitTsFileTransportTask(
      String sourceDir, String sourceTsFileName, String remoteDirPath) {
    return executorService.submit(
        () -> transportTsFile(sourceDir, sourceTsFileName, remoteDirPath));
  }

  private void transportTsFile(String sourceDir, String sourceTsFileName, String remoteDirPath) {}
}
