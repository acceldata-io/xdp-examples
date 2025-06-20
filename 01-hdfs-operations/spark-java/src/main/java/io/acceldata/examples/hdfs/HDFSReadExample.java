    package io.acceldata.examples.hdfs;// ... package and imports remain the same

    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.security.UserGroupInformation;
    import org.apache.spark.SparkConf;
    import org.apache.spark.api.java.JavaRDD;
    import org.apache.spark.api.java.JavaSparkContext;
    import org.apache.spark.sql.Dataset;
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.SparkSession;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.io.File;
    import java.io.IOException;

    public class HDFSReadExample {

        private static final Logger logger = LoggerFactory.getLogger(HDFSReadExample.class);

        public static void main(String[] args) {
            String hdfsPath = args.length > 0 ? args[0] : System.getenv().getOrDefault("HDFS_PATH", "hdfs://qenamenode1:8020/user/nitin-upadhyaya/");
            String principal = System.getenv().getOrDefault("KERBEROS_PRINCIPAL", "hdfs-adocqecluster@ADSRE.COM");
            String keytabPath = System.getenv().getOrDefault("KERBEROS_KEYTAB", "/etc/user.keytab");
            SparkConf conf = new SparkConf()
                    .setAppName("HDFS Read Example - Acceldata")
                    .set("spark.sql.adaptive.enabled", "true")
                    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.kryoserializer.buffer.max", "512m")
                    .set("spark.hadoop.hadoop.security.authentication", "kerberos")
                    .set("spark.hadoop.hadoop.security.authorization", "true")
                    .set("spark.yarn.principal", principal)
                    .set("spark.yarn.keytab", keytabPath);


            // Allow override of master for testing outside cluster
    //        if (!conf.contains("spark.master")) {
    //            conf.setMaster("local[*]");
    //        }

            SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
            configureHadoopForContainer(spark);
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

            try {
                logger.info("Starting HDFS Read Operations Example");
                logger.info("HDFS Path: {}", hdfsPath);
                readTextFileAsRDD(jsc, hdfsPath + "/file1.txt");
                logger.info("HDFS Read Operations completed successfully");
            } catch (Exception e) {
                logger.error("Error during HDFS read operations", e);
                System.exit(1);
            } finally {
                spark.stop();
            }
        }

        private static void readTextFileAsRDD(JavaSparkContext jsc, String filePath) {
            try {
                logger.info("Reading text file as RDD: {}", filePath);
                JavaRDD<String> textRDD = jsc.textFile(filePath);
                long lineCount = textRDD.count();
                logger.info("Total lines in file: {}", lineCount);
                JavaRDD<String> nonEmptyLines = textRDD.filter(line -> !line.trim().isEmpty());
                logger.info("Non-empty lines: {}", nonEmptyLines.count());
                logger.info("First 5 lines:");
                nonEmptyLines.take(5).forEach(line -> logger.info("  {}", line));
                JavaRDD<String> words = textRDD.flatMap(line -> java.util.Arrays.asList(line.split("\\s+")).iterator());
                long wordCount = words.count();
                logger.info("Total words: {}", wordCount);
            } catch (Exception e) {
                logger.warn("Could not read text file: {} - {}", filePath, e.getMessage());
            }
        }

        private static void configureHadoopForContainer(SparkSession spark) {
            try {
                logger.info("Configuring Hadoop for containerized environment");

                Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();

                // Allow overriding fs.defaultFS via ENV
                String fsDefault = System.getenv().getOrDefault("FS_DEFAULT", "hdfs://qenamenode1:8020");
                hadoopConf.set("fs.defaultFS", fsDefault);

                // Load config files (assumed mounted into /config/)
                hadoopConf.addResource(new File("/etc/hadoop/conf/core-site.xml").toURI().toURL());
                hadoopConf.addResource(new File("/etc/hadoop/conf/hdfs-site.xml").toURI().toURL());
                hadoopConf.addResource(new File("/etc/hadoop/conf/hive-site.xml").toURI().toURL());

                configureKerberosAuthentication(hadoopConf);
                logger.info("✓ Hadoop configuration completed for containerized environment");
            } catch (Exception e) {
                logger.error("Failed to configure Hadoop for container environment", e);
                throw new RuntimeException("Hadoop configuration failed", e);
            }
        }

        private static void configureKerberosAuthentication(Configuration hadoopConf) throws IOException {
            logger.info("Configuring Kerberos authentication");

            String principal = System.getenv().getOrDefault("KERBEROS_PRINCIPAL", "hdfs-adocqecluster@ADSRE.COM");
            String keytabPath = System.getenv().getOrDefault("KERBEROS_KEYTAB", "/etc/user.keytab");
            String krb5Path = System.getenv().getOrDefault("KRB5_CONFIG", "/etc/krb5.conf");

            if (principal == null || keytabPath == null) {
                throw new RuntimeException("KERBEROS_PRINCIPAL or KERBEROS_KEYTAB environment variables are not set.");
            }

            File keytabFile = new File(keytabPath);
            if (!keytabFile.exists()) {
                throw new RuntimeException("Keytab file not found at: " + keytabPath);
            }

            File krb5File = new File(krb5Path);
            if (!krb5File.exists()) {
                throw new RuntimeException("Kerberos configuration file not found at: " + krb5Path);
            }

            System.setProperty("java.security.krb5.conf", krb5Path);

            hadoopConf.set("hadoop.security.authentication", "kerberos");
            hadoopConf.set("hadoop.security.authorization", "true");

            UserGroupInformation.setConfiguration(hadoopConf);
            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

            logger.info("✓ Kerberos authentication configured");
            logger.info("  Principal: {}", principal);
            logger.info("  Keytab: {}", keytabPath);
            logger.info("  Current user: {}", UserGroupInformation.getCurrentUser());
        }
    }
