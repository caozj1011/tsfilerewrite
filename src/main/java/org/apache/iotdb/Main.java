package org.apache.iotdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

  private static final int VSG_NUM = 20;

  public static void main(String[] args) {
    Config config = new Config();
    String[] sourceDirPathList = config.getSourceDirPathList();
    String loadDirPath = config.getLoadDirPath();
    File hardLinkDir = new File(loadDirPath);
    if (!hardLinkDir.exists()) {
      hardLinkDir.mkdir();
    }

    ExecutorService executorService = Executors.newFixedThreadPool(VSG_NUM);
    for (int k = 0; k < VSG_NUM; k++) {
      int finalK = k;
      executorService.submit(
          () -> {
            processVSG(finalK, sourceDirPathList, loadDirPath);
          });
    }

    executorService.shutdown();
    while (true) {
      if (executorService.isTerminated()) {
        break;
      }
    }
  }

  private static void processVSG(int vsgIndex, String[] sourceDirPathList, String loadDirPath) {
    String vsgLoadDirPath = loadDirPath + File.separator + vsgIndex;
    File vsgLoadDir = new File(vsgLoadDirPath);
    if (!vsgLoadDir.exists()) {
      vsgLoadDir.mkdir();
    }

    List<File> tsFileList = new ArrayList<>(5000);
    for (String sourceDirPath : sourceDirPathList) {
      File sourceDir = new File(sourceDirPath);
      if (!sourceDir.exists()) {
        System.out.println(sourceDirPath + "not exists.");
        continue;
      }
      File[] tsFiles = sourceDir.listFiles();
      if (tsFiles != null) {
        tsFileList.addAll(Arrays.asList(tsFiles));
      }
    }

    tsFileList.sort(Comparator.comparing(File::getName));

    processTsFile(tsFileList, vsgLoadDir);
  }

  private static void processTsFile(List<File> tsFileList, File vsgLoadDir) {}
}
