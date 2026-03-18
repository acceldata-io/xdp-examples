import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.conf.Configuration;

public class Job2S3Write {
    public static void main(String[] args) {

        // ==========================
        // Environment Variables (Driver)
        // ==========================
        String accessKey = System.getenv().getOrDefault("DATASTORE_AWS_ACCESS_KEY_ID", "");
        String secretKey = System.getenv().getOrDefault("DATASTORE_AWS_SECRET_ACCESS_KEY", "");
        String bucketName = System.getenv().getOrDefault("DATASTORE_S3_BUCKET_NAME", "");
        String s3FilePath = System.getenv().getOrDefault("DATASTORE_S3_FILE_PATH", "");
        String s3FilePathOutput = System.getenv().getOrDefault("DATASTORE_S3_FILE_PATH_OUTPUT", "");
        String s3Region = System.getenv().getOrDefault("DATASTORE_S3_REGION", "");

        // ==========================
        // Initialize Spark Session
        // ==========================
        SparkSession spark = SparkSession.builder()
                .appName("S3WriteApplication")
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

        // ==========================
        // Build Output File Path
        // ==========================
        String outPutFilePath = "s3a://" + bucketName + "/" + s3FilePathOutput;

        // ==========================
        // Write Data to S3
        // ==========================
        df.write()
                .option("header", "true")
                .mode("overwrite")
                .csv(outPutFilePath);

        // ==========================
        // Read Data from S3
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
