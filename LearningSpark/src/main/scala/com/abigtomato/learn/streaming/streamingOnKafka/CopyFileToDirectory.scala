package com.abigtomato.spark.scala.streaming.streamingOnKafka

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.UUID

/**
  * 将项目中的 ./data/copyFileWord 文件 每隔5s 复制到 ./data/streamingCopyFile 路径下
  */

object CopyFileToDirectory {
  def main(args: Array[String]): Unit = {
    while(true){
      Thread.sleep(5000);
      val uuid = UUID.randomUUID().toString();
      println(uuid);
      copyFile(new File("./data/copyFileWord"),new File(".\\data\\streamingCopyFile\\"+uuid+"----words.txt"));
    }
  }

  /**
    * 复制文件到文件夹目录下
    */
  def copyFile(fromFile: File, toFile: File): Unit ={
    val ins = new FileInputStream(fromFile);
    val out = new FileOutputStream(toFile);
    val buffer = new Array[Byte](1024*1024)
    var size = 0
    while (size != -1) {
      out.write(buffer, 0, buffer.length);
      size = ins.read(buffer)
    }
    ins.close();
    out.close();
  }

}
