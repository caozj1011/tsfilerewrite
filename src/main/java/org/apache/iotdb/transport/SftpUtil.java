package org.apache.iotdb.transport;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Vector;

/**
 * FTP服务器工具类： JSch类 通过 SFTP 协议上传文件到 freeSSHd 服务器
 *
 * @author yindong
 * @time 2019-05-28
 * @vision 1.0.0
 */
public class SftpUtil {

  private Logger logger = LoggerFactory.getLogger(SftpUtil.class);

  private ChannelSftp sftp;

  private Session session;

  /** 用户名 */
  private String username;

  /** 密码 */
  private String password;

  /** 秘钥 */
  private String privateKey;

  /** FTP服务器Ip */
  private String host;

  /** FTP服务器端口号 */
  private int port;

  /**
   * 构造器：基于密码认证sftp对象
   *
   * @param username 用户名
   * @param password 密码
   * @param host 服务器ip
   * @param port 服务器端口号
   */
  public SftpUtil(String username, String password, String host, int port) {
    this.username = username;
    this.password = password;
    this.host = host;
    this.port = port;
  }

  /** 连接SFTP服务器 */
  public void login() {
    JSch jsch = new JSch();
    try {
      if (privateKey != null) {
        // 设置登陆主机的秘钥
        jsch.addIdentity(privateKey);
      }
      // 采用指定的端口连接服务器
      session = jsch.getSession(username, host, port);
      if (password != null) {
        // 设置登陆主机的密码
        session.setPassword(password);
      }
      // 优先使用 password 验证   注：session.connect()性能低，使用password验证可跳过gssapi认证，提升连接服务器速度
      session.setConfig("PreferredAuthentications", "password");
      // 设置第一次登陆的时候提示，可选值：(ask | yes | no)
      session.setConfig("StrictHostKeyChecking", "no");
      session.connect();
      // 创建sftp通信通道
      Channel channel = session.openChannel("sftp");
      channel.connect();
      sftp = (ChannelSftp) channel;
      logger.info("sftp server connect success !!");
    } catch (JSchException e) {
      logger.error("SFTP服务器连接异常！！", e);
    }
  }

  /** 关闭SFTP连接 */
  public void logout() {
    if (sftp != null) {
      if (sftp.isConnected()) {
        sftp.disconnect();
        logger.info("sftp is close already");
      }
    }
    if (session != null) {
      if (session.isConnected()) {
        session.disconnect();
        logger.info("session is close already");
      }
    }
  }

  /**
   * 将输入流上传到SFTP服务器，作为文件
   *
   * @param directory 上传到SFTP服务器的路径
   * @param sftpFileName 上传到SFTP服务器后的文件名
   * @param input 输入流
   * @throws SftpException
   */
  public void upload(String directory, String sftpFileName, InputStream input)
      throws SftpException {
    long start = System.currentTimeMillis();
    try {
      // 如果文件夹不存在，则创建文件夹
      if (sftp.ls(directory) == null) {
        sftp.mkdir(directory);
      }
      // 切换到指定文件夹
      sftp.cd(directory);
    } catch (SftpException e) {
      // 创建不存在的文件夹，并切换到文件夹
      sftp.mkdir(directory);
      sftp.cd(directory);
    }
    sftp.put(input, sftpFileName);
    logger.info("文件上传成功！！ 耗时：{}ms", (System.currentTimeMillis() - start));
  }

  /**
   * 上传单个文件
   *
   * @param directory 上传到SFTP服务器的路径
   * @param uploadFileUrl 文件路径
   */
  public void upload(String directory, String uploadFileUrl) {
    File file = new File(uploadFileUrl);
    try {
      upload(directory, file.getName(), new FileInputStream(file));
    } catch (FileNotFoundException | SftpException e) {
      logger.error("上传文件异常！", e);
    }
  }

  /**
   * 删除文件夹
   *
   * @param directory SFTP服务器的文件路径
   */
  public void delete(String directory) {
    Vector vector = listFiles(directory);
    vector.remove(0);
    vector.remove(0);
    for (Object v : vector) {
      ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) v;
      try {
        sftp.cd(directory);
        sftp.rm(lsEntry.getFilename());
      } catch (SftpException e) {
        logger.error("文件删除异常！", e);
      }
    }
  }

  /**
   * 获取文件夹下的文件
   *
   * @param directory 路径
   * @return
   */
  public Vector<?> listFiles(String directory) {
    try {
      if (isDirExist(directory)) {
        Vector<?> vector = sftp.ls(directory);
        // 移除上级目录和根目录："." ".."
        vector.remove(0);
        vector.remove(0);
        return vector;
      }
    } catch (SftpException e) {
      logger.error("获取文件夹信息异常！", e);
    }
    return null;
  }

  /**
   * 创建一个文件目录
   *
   * @param createpath 路径
   * @return
   */
  public boolean createDir(String createpath) {
    try {
      if (isDirExist(createpath)) {
        this.sftp.cd(createpath);
        return true;
      }
      String pathArry[] = createpath.split("/");
      StringBuffer filePath = new StringBuffer("/");
      for (String path : pathArry) {
        if (path.equals("")) {
          continue;
        }
        filePath.append(path + "/");
        if (isDirExist(filePath.toString())) {
          sftp.cd(filePath.toString());
        } else {
          // 建立目录
          sftp.mkdir(filePath.toString());
          // 进入并设置为当前目录
          sftp.cd(filePath.toString());
        }
      }
      this.sftp.cd(createpath);
    } catch (SftpException e) {
      logger.error("目录创建异常！", e);
      return false;
    }
    return true;
  }

  /**
   * 判断目录是否存在
   *
   * @param directory 路径
   * @return
   */
  public boolean isDirExist(String directory) {
    boolean isDirExistFlag = false;
    try {
      SftpATTRS sftpATTRS = this.sftp.lstat(directory);
      isDirExistFlag = true;
      return sftpATTRS.isDir();
    } catch (Exception e) {
      if (e.getMessage().toLowerCase().equals("no such file")) {
        isDirExistFlag = false;
      }
    }
    return isDirExistFlag;
  }
}
