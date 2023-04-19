package org.apache.iotdb.load;

import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TsFileLoadManager {

  private ExecutorService executorService;

  private ISessionPool sessionPool;

  public TsFileLoadManager() {}

  public void init(int concurrentNum, String host, int port, String user, String pwd) {
    if (executorService == null) {
      executorService = Executors.newFixedThreadPool(concurrentNum);
    }
    sessionPool = new SessionPool.Builder().host(host).port(port).user(user).password(pwd).build();
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
    sessionPool.close();
  }

  public Future<?> submitTsFileLoadTask(String sourceTsFileName, String remoteDirPath) {
    return executorService.submit(
        () -> {
          loadTsFile(sourceTsFileName, remoteDirPath);
        });
  }

  private void loadTsFile(String sourceTsFileName, String remoteDirPath) {
    try {
      sessionPool.executeNonQueryStatement(
          String.format("Load '%s'", remoteDirPath + File.separator + sourceTsFileName));
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }
}
