package com.liu

/**
  * Created by liubin on 2017/4/15.
  */

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkContext,SparkConf}

object WordCount {
  def main(args:Array[String]): Unit ={
    val in="/QuaraQuestionPairs/questions.csv"
    val master="hdfs://192.168.1.101:9000"
    val out="/QuaraQuestionPairs/out"
    deleteFile(master,out)
    countWord(master+in,master+out)
  }

  def countWord(inpath:String,outpath:String): Unit ={
    val conf=new SparkConf().setAppName("WordCount")
    val sc=new SparkContext(conf)
    val lines=sc.textFile(inpath)
    val words=lines.flatMap(line=>splitline(line).split(" "))
    val counts=words.map(word=>(word,1)).reduceByKey((x,y)=>x+y)
    counts.saveAsTextFile(outpath)
  }

  //define a function for split line into words
  def splitline(line:String):String={
    var res:String=""
    var arr:Array[String]=line.split(",")
    if(arr.length==6)
      res=arr(3)+" "+arr(4)
    return res
  }

  //delete the output directory so that don't need to delete it manually
  def deleteFile(master:String,path:String): Unit={
    println("Begin delete!"+master+path)
    val out=new Path(master+path)
    val hdfs=org.apache.hadoop.fs.FileSystem.get(new URI(master),new Configuration())
    if(hdfs.exists(out)){
      hdfs.delete(out,true)
      println("Delete!--"+master+path)
    }
  }
}