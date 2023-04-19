package org.apache.iotdb.rewrite;

import org.apache.iotdb.TsFileRewrite;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TsFileRewriteManager {

  private ExecutorService executorService;

  public TsFileRewriteManager() {}

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

  public Future<?> submitTsFileRewriteTask(File sourceTsFile, File rewriteDir) {
    return executorService.submit(
        () -> {
          try {
            new TsFileRewrite(sourceTsFile, rewriteDir).parseAndRewriteFile();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void rewriteTsFile(File sourceTsFile, File rewriteDir) {}
}
