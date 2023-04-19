package org.apache.iotdb;

import java.util.ArrayList;
import java.util.List;

public class PathUtil {

  public static List<String> split(String path) {
    List<String> nodes = new ArrayList<>();
    int index;
    int nextIndex = 0;
    while (nextIndex < path.length()) {
      if (path.charAt(nextIndex) == '\"') {
        index = path.indexOf('\"', nextIndex + 1) + 1;
      } else {
        index = path.indexOf('.', nextIndex + 1);
        if (index == -1) {
          index = path.length();
        }
      }
      nodes.add(path.substring(nextIndex, index));
      nextIndex = index + 1;
    }
    return nodes;
  }

  public static String transformNodes(String node) {
    if (node.charAt(0) == '\"') {
      return '`' + node.substring(1, node.length() - 1) + '`';
    } else {
      return '`' + node + '`';
    }
  }

  public static String transformPath(String path, int index) {
    List<String> nodes = split(path);
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(nodes.get(0));
    stringBuilder.append(".bw.baoshan");
    if (index > 2) {
      stringBuilder.append(".").append(nodes.get(2));
    }
    for (int i = index; i < nodes.size(); i++) {
      stringBuilder.append('.').append(transformNodes(nodes.get(i)));
    }
    return stringBuilder.toString();
  }

  public static String wrapPath(String path) {
    List<String> nodes = split(path);
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(nodes.get(0));
    for (int i = 1; i < nodes.size(); i++) {
      if (nodes.get(i).charAt(0) == '"') {
        stringBuilder.append('.').append(nodes.get(i));
      } else {
        stringBuilder.append('.').append('`').append(nodes.get(i)).append('`');
      }
    }
    return stringBuilder.toString();
  }
}
