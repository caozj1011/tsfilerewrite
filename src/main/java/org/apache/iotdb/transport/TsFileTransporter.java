package org.apache.iotdb.transport;

public class TsFileTransporter {

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
    sftpUtil.upload(remoteDirPath, sourceTsFilePath);
  }
}
