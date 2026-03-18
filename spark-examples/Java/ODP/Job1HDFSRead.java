import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class Job1HDFSRead {
    public static void main(String[] args) throws Exception {

        // ==========================
        // Environment Variables (Driver)
        // ==========================
        String hdfsUrl = System.getenv().getOrDefault("URL", "");
        String kerberosPrincipal = System.getenv().getOrDefault("KERBEROS_PRINCIPAL", "");
        String kerberosKeytab = System.getenv().getOrDefault("KERBEROS_KEYTAB", "");
        String hdfsFilePath = System.getenv().getOrDefault("HDFS_FILE_PATH", "");

        // ==========================
        // Initialize Spark Session
        // ==========================
        SparkSession spark = SparkSession.builder()
                .appName("HDFSReadApplication")
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

        spark.stop();
    }
}
