package org.apache.iotdb;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

public class Config {

  private static final String PROPERTIES_FILE_NAME = "tsfile-rewrite.properties";

  private String[] sourceDirPathList;

  private String rewriteDirPath;

  private String loadDirPath;

  private String targetIoTDBHost = "127.0.0.1";
  private int targetIoTDBPort = 6668;
  private String targetIoTDBUser = "root";
  private String targetIoTDBPwd = "root";

  private String targetServerHost = "127.0.0.1";
  private int targetServerPort = 6668;
  private String targetServerUser = "root";
  private String targetServerPwd = "root";

  public Config() {
    URL url = Main.class.getProtectionDomain().getCodeSource().getLocation();
    try {
      String path = URLDecoder.decode(url.getPath(), "utf-8");
      if (path.endsWith("jar")) {
        path = path.substring(0, path.length() - "tsfile-rewrite-0.1.0-SNAPSHOT.jar".length() - 1);
      }
      url = new URL("file:" + path + File.separator + PROPERTIES_FILE_NAME);
      System.out.println(url.getPath());
    } catch (UnsupportedEncodingException | MalformedURLException e) {
      throw new RuntimeException(e);
    }
    Properties properties = new Properties();
    try (InputStream inputStream = url.openStream()) {
      properties.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    loadProperties(properties);
    properties.list(System.out);
  }

  private void loadProperties(Properties properties) {
    sourceDirPathList = properties.getProperty("source_dir").split(",");
    rewriteDirPath = properties.getProperty("rewrite_dir");
    loadDirPath = properties.getProperty("load_dir");

    this.targetIoTDBHost = properties.getProperty("target_iotdb_host");
    this.targetIoTDBPort = Integer.parseInt(properties.getProperty("target_iotdb_port"));
    this.targetIoTDBUser = properties.getProperty("target_iotdb_user");
    this.targetIoTDBPwd = properties.getProperty("target_iotdb_pwd");

    this.targetServerHost = properties.getProperty("target_server_host");
    this.targetServerPort = Integer.parseInt(properties.getProperty("target_server_port"));
    this.targetServerUser = properties.getProperty("target_server_user");
    this.targetServerPwd = properties.getProperty("target_server_pwd");
  }

  public String[] getSourceDirPathList() {
    return sourceDirPathList;
  }

  public String getRewriteDirPath() {
    return rewriteDirPath;
  }

  public String getLoadDirPath() {
    return loadDirPath;
  }

  public String getTargetIoTDBHost() {
    return targetIoTDBHost;
  }

  public int getTargetIoTDBPort() {
    return targetIoTDBPort;
  }

  public String getTargetIoTDBUser() {
    return targetIoTDBUser;
  }

  public String getTargetIoTDBPwd() {
    return targetIoTDBPwd;
  }

  public String getTargetServerHost() {
    return targetServerHost;
  }

  public int getTargetServerPort() {
    return targetServerPort;
  }

  public String getTargetServerUser() {
    return targetServerUser;
  }

  public String getTargetServerPwd() {
    return targetServerPwd;
  }
}
