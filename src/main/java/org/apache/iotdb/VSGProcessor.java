package org.apache.iotdb;

import org.apache.iotdb.load.TsFileLoadManager;
import org.apache.iotdb.rewrite.TsFileRewriteManager;
import org.apache.iotdb.transport.TsFileTransportManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class VSGProcessor {

  private final int vsgIndex;
  private final String[] vsgSourceDirPathList;
  private final String vsgRewriteDirPath;
  private final String vsgLoadDirPath;

  private final TsFileRewriteManager tsFileRewriteManager;
  private final TsFileTransportManager tsFileTransportManager;
  private final TsFileLoadManager tsFileLoadManager;

  public VSGProcessor(
      int vsgIndex,
      String[] sourceDirPathList,
      String rewriteDirPath,
      String loadDirPath,
      TsFileRewriteManager tsFileRewriteManager,
      TsFileTransportManager tsFileTransportManager,
      TsFileLoadManager tsFileLoadManager) {
    this.vsgIndex = vsgIndex;

    this.vsgSourceDirPathList = new String[sourceDirPathList.length];
    for (int i = 0; i < sourceDirPathList.length; i++) {
      vsgSourceDirPathList[i] = sourceDirPathList[i] + File.separator + vsgIndex;
    }

    this.vsgRewriteDirPath = rewriteDirPath + File.separator + vsgIndex;
    this.vsgLoadDirPath = loadDirPath + File.separator + vsgIndex;

    this.tsFileRewriteManager = tsFileRewriteManager;
    this.tsFileTransportManager = tsFileTransportManager;
    this.tsFileLoadManager = tsFileLoadManager;
  }

  public void execute() {
    File vsgRewriteDir = new File(vsgLoadDirPath);
    if (!vsgRewriteDir.exists()) {
      vsgRewriteDir.mkdir();
    }

    processTsFile(getSequenceTsFileList());
    processTsFile(getUnSequenceTsFileList());
  }

  private List<File> getSequenceTsFileList() {
    List<File> tsFileList = new ArrayList<>(5000);
    for (String sourceDirPath : vsgSourceDirPathList) {
      sourceDirPath = sourceDirPath + File.separator + "sequence";
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
    return tsFileList;
  }

  private List<File> getUnSequenceTsFileList() {
    List<File> tsFileList = new ArrayList<>(5000);
    for (String sourceDirPath : vsgSourceDirPathList) {
      sourceDirPath = sourceDirPath + File.separator + "unsequence";
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

    return tsFileList;
  }

  private void processTsFile(List<File> tsFileList) {
    if (tsFileList.isEmpty()) {
      return;
    }

    File rewriteDir = new File(vsgRewriteDirPath);
    if (!rewriteDir.exists()) {
      rewriteDir.mkdir();
    }

    Future<?> rewriteFuture = null;
    Future<?> transportFuture = null;
    Future<?> loadFuture = null;
    int rewriteIndex = 0, transportIndex = 0, loadIndex = 0;

    rewriteFuture =
        tsFileRewriteManager.submitTsFileRewriteTask(tsFileList.get(rewriteIndex), rewriteDir);
    while (rewriteIndex < tsFileList.size()) {
      if (rewriteFuture != null) {
        try {
          rewriteFuture.get();
          rewriteIndex++;
          rewriteFuture = null;
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }

      if (rewriteIndex < tsFileList.size()) {
        rewriteFuture =
            tsFileRewriteManager.submitTsFileRewriteTask(tsFileList.get(rewriteIndex), rewriteDir);
      }

      if (transportFuture != null) {
        try {
          transportFuture.get();
          transportIndex++;
          transportFuture = null;
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }

      if (transportIndex < rewriteIndex) {
        transportFuture =
            tsFileTransportManager.submitTsFileTransportTask(
                vsgRewriteDirPath, tsFileList.get(transportIndex).getName(), vsgLoadDirPath);
      }

      if (loadFuture != null) {
        try {
          loadFuture.get();
          tsFileList.get(loadIndex).delete();
          loadIndex++;
          loadFuture = null;
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
      if (loadIndex < transportIndex) {
        loadFuture =
            tsFileLoadManager.submitTsFileLoadTask(
                tsFileList.get(loadIndex).getName(), vsgLoadDirPath);
      }
    }
  }
}
