import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.hadoop.security.UserGroupInformation

object Job2HDFSWrite {
  def main(args: Array[String]): Unit = {

    // ==========================
    // Environment Variables (Driver)
    // ==========================
    val hdfsUrl = sys.env.getOrElse("URL", "")
    val kerberosPrincipal = sys.env.getOrElse("KERBEROS_PRINCIPAL", "")
    val kerberosKeytab = sys.env.getOrElse("KERBEROS_KEYTAB", "")
    val hdfsFilePath = sys.env.getOrElse("HDFS_FILE_PATH", "")
    val hdfsFilePathOutput = sys.env.getOrElse("HDFS_FILE_PATH_OUTPUT", "") 
    

    
    println(s"HDFS URL: $hdfsUrl")
    println(s"Kerberos Principal: $kerberosPrincipal")
    println(s"Kerberos Keytab: $kerberosKeytab")

    // ==========================
    // Initialize Spark Session
    // ==========================
    val spark = SparkSession.builder()
      .appName("HDFSWriteApplication")
      .config("spark.kerberos.keytab", kerberosKeytab)
      .config("spark.kerberos.principal", kerberosPrincipal)
      .getOrCreate()

    // ==========================
    // Authenticate with Kerberos
    // ==========================
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("hadoop.security.authentication", "kerberos")
    hadoopConf.set("hadoop.security.authorization", "true")

    UserGroupInformation.setConfiguration(hadoopConf)
    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab)

    // ==========================
    // Build HDFS File Path
    // ==========================
    val filePath = s"$hdfsUrl/$hdfsFilePath" 

    // ==========================
    // Read Data from HDFS
    // ==========================
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)


    // ==========================
    // Show the data
    // ==========================
    df.show(truncate = false)

    // ==========================
    // Build Output File Path 
    // ==========================
    val outPutFilePath = s"$hdfsUrl/$hdfsFilePathOutput"

    // ==========================
    // Write  the file 
    // ==========================
    df.write
      .option("header", "true")
      .mode("overwrite")
      .csv(outPutFilePath)

    // ==========================
    // Read Data from HDFS New File Path 
    // ==========================
    val df_output = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(outPutFilePath)


    // ==========================
    // Show the data
    // ==========================
    df_output.show(truncate = false)


    spark.stop()
  }
}