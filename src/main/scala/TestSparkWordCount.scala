import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TestSparkWordCount {
  def main(args: Array[String]): Unit = {
    
    // val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testsparkwordcount")
    
     val sparkConf = new SparkConf().setAppName("sparkwordcount")
     sparkConf.set("spark.hadoop.fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem")
     sparkConf.set("spark.hadoop.fs.AbstractFileSystem.adl.impl","org.apache.hadoop.fs.adl.Adl")
     sparkConf.set("spark.mesos.executor.docker.forcePullImage", "true")
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
     
     //set correct values from you azure web app, these are dummys
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.credential", "ll5453452ert23Kvc=")
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.client.id", "153201-2r2344-7bb-b745-2effqw2435")
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/7wer88bfqwerwer/oauth2/token")

     val sc = new SparkContext(sparkConf)
     
    val inF = sc.textFile("adl://acs.azuredatalakestore.net/README.md") 
    
    val wc = inF.flatMap(line => line.split(' ')).map( word => (word,1))
    wc.reduceByKey(_ + _).collect().foreach(println) 
    println("total words ===> " + wc.count())
  }
}