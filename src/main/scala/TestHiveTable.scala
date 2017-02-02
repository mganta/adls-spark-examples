import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object TestHiveTable {
  def main(args: Array[String]): Unit = {

    //fill with real values
    val spark = SparkSession
      .builder()
      .appName("Spark Hive ADLS Example")
      .config("hive.metastore.uris", "thrift://9.0.7.10:9083")
      .config("spark.hadoop.fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.adl.impl","org.apache.hadoop.fs.adl.Adl")
      .config("spark.mesos.executor.docker.forcePullImage", "true")
      .config("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
      .config("spark.hadoop.dfs.adls.oauth2.credential", "afdfK")
      .config("spark.hadoop.dfs.adls.oauth2.client.id", "15643201-asdf23412")
      .config("spark.hadoop.dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f9523df222554/oauth2/token") 
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    // Queries are expressed in HiveQL
    sql("SELECT * FROM drivers").show()

    sql("SELECT COUNT(*) FROM drivers").show()
  }
}