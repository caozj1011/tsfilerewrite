package org.apache.iotdb.transport;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TsFileTransporter {

  private static Lock lock = new ReentrantLock();

  private SftpUtil sftpUtil;

  private boolean isUsed = false;

  public TsFileTransporter(String host, int port, String user, String pwd) {
    sftpUtil = new SftpUtil(user, pwd, host, port);
  }

  public void clear() {
    sftpUtil.logout();
  }

  public void init() {
    sftpUtil.login();
  }

  public boolean isUsed() {
    return isUsed;
  }

  public void setUsed(boolean isUsed) {
    this.isUsed = isUsed;
  }

  public void upLoadFile(String remoteDirPath, String sourceTsFilePath) {
    if (!sftpUtil.isDirExist(remoteDirPath)) {
      lock.lock();
      try {
        sftpUtil.createDir(remoteDirPath);
      } finally {
        lock.unlock();
      }
    }
    sftpUtil.upload(remoteDirPath, sourceTsFilePath);
  }
}
