import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.conf.Configuration;

public class Job1S3Read {
    public static void main(String[] args) {

        // ==========================
        // Environment Variables (Driver)
        // ==========================
        String accessKey = System.getenv().getOrDefault("DATASTORE_AWS_ACCESS_KEY_ID", "");
        String secretKey = System.getenv().getOrDefault("DATASTORE_AWS_SECRET_ACCESS_KEY", "");
        String bucketName = System.getenv().getOrDefault("DATASTORE_S3_BUCKET_NAME", "");
        String s3FilePath = System.getenv().getOrDefault("DATASTORE_S3_FILE_PATH", "");
        String s3Region = System.getenv().getOrDefault("DATASTORE_S3_REGION", "");

        // ==========================
        // Initialize Spark Session
        // ==========================
        SparkSession spark = SparkSession.builder()
                .appName("S3ReadApplication")
                .getOrCreate();

        // ==========================
        // Configure S3 credentials in Hadoop configuration
        // ==========================
        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        hadoopConf.set("fs.s3a.access.key", accessKey);
        hadoopConf.set("fs.s3a.secret.key", secretKey);
        hadoopConf.set("fs.s3a.endpoint.region", s3Region);

        // Optional: specify S3 endpoint if using non-AWS S3
        // hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com");

        // ==========================
        // Build S3 File Path
        // ==========================
        String filePath = "s3a://" + bucketName + "/" + s3FilePath;

        // ==========================
        // Read Data from S3
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
