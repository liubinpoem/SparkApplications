/**
  * Created by liubin on 2017/4/20.
  */

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.Random

object AvgPeopleAge {

  //delete the output directory so that don't need to delete it manually
  def DeleteFile(master:String,path:String): Unit={
    println("Begin delete!"+master+path)
    val out=new Path(master+path)
    val hdfs=org.apache.hadoop.fs.FileSystem.get(new URI(master),new Configuration())
    if(hdfs.exists(out)){
      hdfs.delete(out,true)
      println("------------------------------------------------------------------------------------------------")
      println("Delete!--"+master+path +"--Done!")
      println("------------------------------------------------------------------------------------------------")
    }
  }

  //function of generating sample data
  def SampleDataGenerator(master:String,outpath:String,people:Long): Long ={
    val path=new Path(master+outpath)
    val fs=FileSystem.get(new URI(master),new Configuration())
    val os=fs.create(path)
    //generate random  sample ages
    val rand=new Random()
    val count:Long=0L
    for (count <- 1L to people) {
      os.writeBytes(count + " " + rand.nextInt(100) + "\n")
    }
    println("------------------------------------------------------------------------------------------------")
    println("Create Data Done! Write "+count+" People Ages to File")
    println("------------------------------------------------------------------------------------------------")
    return count
  }

  def main(args:Array[String]): Unit ={
    val master="hdfs://192.168.1.101:9000"
    val datapath="/SparkApplications/AvgPeopleAge/age_1000000000.txt"
    //十亿的数据量
    val people:Long=1000000000L
    //Delete the file if exists
    //DeleteFile(master,datapath)
    //generate sample data
    //val count=SampleDataGenerator(master,datapath,people)
    //calculate the avara
    val start_time:Long=System.currentTimeMillis()
    val avgAgeRes:Array[Double]=CalculateAverage(master,datapath,people)
    val end_time:Long=System.currentTimeMillis()
    println("------------------------------------------------------------------------------------------------")
    println("Parameter count:"+avgAgeRes(0))
    println("Parameter people :"+people)
    println("Average Age of People is "+avgAgeRes(1))
    println("Time consumption of processing such huge file is "+(end_time-start_time)/60000+" minutes")
    println("------------------------------------------------------------------------------------------------")
  }

  //calculate the average ages of people
  def CalculateAverage(master:String,datapath:String,people:Long):Array[Double]={
    val res:Array[Double]=new Array[Double](2)
    val conf=new SparkConf().setAppName("AvgPeopleAge")
    val sc=new SparkContext(conf)
    val dataFile=sc.textFile(master+datapath)
    res(0)=dataFile.count()
    val ageData=dataFile.map(line=>line.split(" ")(1)).repartition(150)
    val totalAge:Long=ageData.map(age=>age.toLong).reduce((a,b)=>a+b)
    res(1)=totalAge.toDouble/res(0).toDouble
    println("------------------------------------------------------------------------------------------------")
    println("Calculate Average Age Done!")
    println("TotalAge:"+totalAge)
    println("TotalPeople:"+res(0))
    println("Original People: "+ people)
    println("------------------------------------------------------------------------------------------------")
    return res
  }
}
