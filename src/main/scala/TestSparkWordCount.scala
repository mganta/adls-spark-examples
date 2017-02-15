import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TestSparkWordCount {
  def main(args: Array[String]): Unit = {
    
        if (args.length != 4) 
      System.exit(1)
      
      val credential = args(0)
      val clientId = args(1)
      val refreshUrl = args(2)
      val fileUrl = args(3)
      
   // val sparkConf = new SparkConf().setMaster("local[2]").setAppName("testsparkwordcount")
    
     val sparkConf = new SparkConf().setAppName("sparkwordcount")
     sparkConf.set("spark.hadoop.fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem")
     sparkConf.set("spark.hadoop.fs.AbstractFileSystem.adl.impl","org.apache.hadoop.fs.adl.Adl")
     sparkConf.set("spark.mesos.executor.docker.forcePullImage", "true")
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
     
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.credential", credential)
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.client.id", clientId)
     sparkConf.set("spark.hadoop.dfs.adls.oauth2.refresh.url", refreshUrl)
   
     val sc = new SparkContext(sparkConf)
     
     val inF = sc.textFile(fileUrl) 
    
    val wc = inF.flatMap(line => line.split(' ')).map( word => (word,1))
    wc.reduceByKey(_ + _).collect().foreach(println) 
    println("total words ===> " + wc.count())
  }
}