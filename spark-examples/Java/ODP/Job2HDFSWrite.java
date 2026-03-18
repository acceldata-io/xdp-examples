import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class Job2HDFSWrite {
    public static void main(String[] args) throws Exception {

        // ==========================
        // Environment Variables (Driver)
        // ==========================
        String hdfsUrl = System.getenv().getOrDefault("URL", "");
        String kerberosPrincipal = System.getenv().getOrDefault("KERBEROS_PRINCIPAL", "");
        String kerberosKeytab = System.getenv().getOrDefault("KERBEROS_KEYTAB", "");
        String hdfsFilePath = System.getenv().getOrDefault("HDFS_FILE_PATH", "");
        String hdfsFilePathOutput = System.getenv().getOrDefault("HDFS_FILE_PATH_OUTPUT", "");

        System.out.println("HDFS URL: " + hdfsUrl);
        System.out.println("Kerberos Principal: " + kerberosPrincipal);
        System.out.println("Kerberos Keytab: " + kerberosKeytab);

        // ==========================
        // Initialize Spark Session
        // ==========================
        SparkSession spark = SparkSession.builder()
                .appName("HDFSWriteApplication")
                .config("spark.kerberos.keytab", kerberosKeytab)
                .config("spark.kerberos.principal", kerberosPrincipal)
                .getOrCreate();

        // ==========================
        // Authenticate with Kerberos
        // ==========================
        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.set("hadoop.security.authentication", "kerberos");
        hadoopConf.set("hadoop.security.authorization", "true");

        UserGroupInformation.setConfiguration(hadoopConf);
        UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);

        // ==========================
        // Build HDFS File Path
        // ==========================
        String filePath = hdfsUrl + "/" + hdfsFilePath;

        // ==========================
        // Read Data from HDFS
        // ==========================
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(filePath);

        // ==========================
        // Show the data
        // ==========================
        df.show(false);

        // ==========================
        // Build Output File Path
        // ==========================
        String outPutFilePath = hdfsUrl + "/" + hdfsFilePathOutput;

        // ==========================
        // Write the file
        // ==========================
        df.write()
                .option("header", "true")
                .mode("overwrite")
                .csv(outPutFilePath);

        // ==========================
        // Read Data from HDFS New File Path
        // ==========================
        Dataset<Row> dfOutput = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(outPutFilePath);

        // ==========================
        // Show the data
        // ==========================
        dfOutput.show(false);

        spark.stop();
    }
}
