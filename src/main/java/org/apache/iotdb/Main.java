package org.apache.iotdb;

import org.apache.iotdb.load.TsFileLoadManager;
import org.apache.iotdb.rewrite.TsFileRewriteManager;
import org.apache.iotdb.transport.TsFileTransportManager;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

  private static final int VSG_NUM = 20;

  public static void main(String[] args) {
    // 1.1 Load，不跨时间分区，2.56G/min, 跨时间分区 0.4G/min
    Config config = new Config();
    String[] sourceDirPathList = config.getSourceDirPathList();
    String rewriteDirPath = config.getRewriteDirPath();
    String loadDirPath = config.getLoadDirPath();
    File rewriteDir = new File(rewriteDirPath);
    if (!rewriteDir.exists()) {
      rewriteDir.mkdir();
    }

    TsFileRewriteManager tsFileRewriteManager = new TsFileRewriteManager();
    tsFileRewriteManager.init(VSG_NUM);
    TsFileTransportManager tsFileTransportManager = new TsFileTransportManager();
    tsFileTransportManager.init(
        VSG_NUM,
        config.getTargetServerHost(),
        config.getTargetServerPort(),
        config.getTargetServerUser(),
        config.getTargetServerPwd());
    TsFileLoadManager tsFileLoadManager = new TsFileLoadManager();
    tsFileLoadManager.init(
        VSG_NUM,
        config.getTargetIoTDBHost(),
        config.getTargetIoTDBPort(),
        config.getTargetIoTDBUser(),
        config.getTargetIoTDBPwd());

    ExecutorService executorService = Executors.newFixedThreadPool(VSG_NUM);
    for (int k = 0; k < VSG_NUM; k++) {
      int finalK = k;
      executorService.submit(
          () -> {
            new VSGProcessor(
                    finalK,
                    sourceDirPathList,
                    rewriteDirPath,
                    loadDirPath,
                    tsFileRewriteManager,
                    tsFileTransportManager,
                    tsFileLoadManager)
                .execute();
          });
    }

    executorService.shutdown();
    while (true) {
      if (executorService.isTerminated()) {
        break;
      }
    }

    tsFileRewriteManager.clear();
    tsFileTransportManager.clear();
    tsFileLoadManager.clear();
  }
}
