import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object TestHiveTable {
  def main(args: Array[String]): Unit = {
    
    if (args.length != 3) 
      System.exit(1)
      
      val credential = args(0)
      val clientId = args(1)
      val refreshUrl = args(2)

    val spark = SparkSession
      .builder()
      .appName("Spark Hive ADLS Example")
      .config("hive.metastore.uris", "thrift://9.0.7.10:9083")
      .config("spark.hadoop.fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.adl.impl","org.apache.hadoop.fs.adl.Adl")
      .config("spark.mesos.executor.docker.forcePullImage", "true")
      .config("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
      .config("spark.hadoop.dfs.adls.oauth2.credential", credential)
      .config("spark.hadoop.dfs.adls.oauth2.client.id", clientId)
      .config("spark.hadoop.dfs.adls.oauth2.refresh.url", refreshUrl) 
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    // Queries are expressed in HiveQL
    sql("SELECT * FROM drivers").show()

    sql("SELECT COUNT(*) FROM drivers").show()
  }
}