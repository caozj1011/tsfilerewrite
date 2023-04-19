package org.apache.iotdb.load;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TsFileLoadManager {

  private ExecutorService executorService;

  public TsFileLoadManager() {}

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

  public Future<?> submitTsFileLoadTask(
      String sourceDir, String sourceTsFileName, String remoteDirPath) {
    return executorService.submit(
        () -> {
          loadTsFile(sourceDir, sourceTsFileName, remoteDirPath);
        });
  }

  private void loadTsFile(String sourceDir, String sourceTsFileName, String remoteDirPath) {}
}
