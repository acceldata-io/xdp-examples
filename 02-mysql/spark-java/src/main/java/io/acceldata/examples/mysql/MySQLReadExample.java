package io.acceldata.examples.mysql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

/**
 * MySQL Read Operations Example
 * 
 * Demonstrates various ways to read data from MySQL using Spark SQL.
 * Includes full table reads, query-based reads, partitioned reads, and joins.
 * 
 * @author Acceldata Platform Team
 * @version 1.0.0
 */
public class MySQLReadExample {
    
    private static final Logger logger = LoggerFactory.getLogger(MySQLReadExample.class);
    
    private final SparkSession spark;
    private final String mysqlUrl;
    private final Properties connectionProps;
    
    public MySQLReadExample(SparkSession spark, String mysqlUrl, Properties connectionProps) {
        this.spark = spark;
        this.mysqlUrl = mysqlUrl;
        this.connectionProps = connectionProps;
    }
    
    /**
     * Read full table from MySQL
     */
    public Dataset<Row> readFullTable(String tableName) {
        try {
            logger.info("Reading full table: {}", tableName);
            
            Dataset<Row> df = spark.read()
                    .jdbc(mysqlUrl, tableName, connectionProps);
            
            logger.info("Table {} - Row count: {}", tableName, df.count());
            logger.info("Table {} - Schema:", tableName);
            df.printSchema();
            
            // Show sample data
            logger.info("Sample data from {}:", tableName);
            df.show(10);
            
            return df;
            
        } catch (Exception e) {
            logger.error("Error reading table: " + tableName, e);
            throw e;
        }
    }
    
    /**
     * Read data with WHERE clause
     */
    public Dataset<Row> readWithQuery(String tableName, String whereClause) {
        try {
            logger.info("Reading table {} with WHERE clause: {}", tableName, whereClause);
            
            String query = String.format("(SELECT * FROM %s WHERE %s) AS filtered_data", 
                                       tableName, whereClause);
            
            Dataset<Row> df = spark.read()
                    .jdbc(mysqlUrl, query, connectionProps);
            
            logger.info("Filtered data - Row count: {}", df.count());
            df.show();
            
            return df;
            
        } catch (Exception e) {
            logger.error("Error reading with query", e);
            throw e;
        }
    }
    
    /**
     * Read data with partitioning for large tables
     */
    public Dataset<Row> readWithPartitioning(String tableName, String partitionColumn, 
                                           long lowerBound, long upperBound, int numPartitions) {
        try {
            logger.info("Reading table {} with partitioning on column: {}", tableName, partitionColumn);
            logger.info("Partition bounds: {} to {}, partitions: {}", lowerBound, upperBound, numPartitions);
            
            Dataset<Row> df = spark.read()
                    .option("partitionColumn", partitionColumn)
                    .option("lowerBound", lowerBound)
                    .option("upperBound", upperBound)
                    .option("numPartitions", numPartitions)
                    .jdbc(mysqlUrl, tableName, connectionProps);
            
            logger.info("Partitioned read - Row count: {}", df.count());
            logger.info("Number of partitions: {}", df.rdd().getNumPartitions());
            
            return df;
            
        } catch (Exception e) {
            logger.error("Error reading with partitioning", e);
            throw e;
        }
    }
    
    /**
     * Perform JOIN operations between tables
     */
    public Dataset<Row> readWithJoin() {
        try {
            logger.info("Performing JOIN between employees and departments");
            
            // Read both tables
            Dataset<Row> employees = readFullTable("employees");
            Dataset<Row> departments = readFullTable("departments");
            
            // Perform JOIN
            Dataset<Row> joinedData = employees
                    .join(departments, employees.col("department_id").equalTo(departments.col("id")))
                    .select(
                            employees.col("id").alias("emp_id"),
                            employees.col("name").alias("emp_name"),
                            employees.col("salary"),
                            departments.col("name").alias("dept_name"),
                            departments.col("budget").alias("dept_budget")
                    );
            
            logger.info("JOIN result - Row count: {}", joinedData.count());
            joinedData.show();
            
            return joinedData;
            
        } catch (Exception e) {
            logger.error("Error performing JOIN", e);
            throw e;
        }
    }
    
    /**
     * Analyze employee data with aggregations
     */
    public void analyzeEmployeeData() {
        try {
            logger.info("Analyzing employee data...");
            
            Dataset<Row> employees = readFullTable("employees");
            
            // Basic statistics
            logger.info("=== Employee Statistics ===");
            employees.describe("salary").show();
            
            // Salary analysis by department
            logger.info("=== Salary Analysis by Department ===");
            Dataset<Row> salaryByDept = employees
                    .groupBy("department_id")
                    .agg(
                            count("*").alias("employee_count"),
                            avg("salary").alias("avg_salary"),
                            min("salary").alias("min_salary"),
                            max("salary").alias("max_salary"),
                            sum("salary").alias("total_salary")
                    )
                    .orderBy(desc("avg_salary"));
            
            salaryByDept.show();
            
            // Salary distribution
            logger.info("=== Salary Distribution ===");
            Dataset<Row> salaryDistribution = employees
                    .withColumn("salary_range", 
                            when(col("salary").lt(60000), "Low")
                            .when(col("salary").lt(80000), "Medium")
                            .otherwise("High"))
                    .groupBy("salary_range")
                    .count()
                    .orderBy("salary_range");
            
            salaryDistribution.show();
            
        } catch (Exception e) {
            logger.error("Error analyzing employee data", e);
            throw e;
        }
    }
    
    /**
     * Calculate department statistics
     */
    public void calculateDepartmentStatistics() {
        try {
            logger.info("Calculating department statistics...");
            
            // Join employees and departments for comprehensive analysis
            Dataset<Row> joinedData = readWithJoin();
            
            // Department efficiency (avg salary vs budget)
            logger.info("=== Department Efficiency Analysis ===");
            Dataset<Row> deptEfficiency = joinedData
                    .groupBy("dept_name", "dept_budget")
                    .agg(
                            count("*").alias("employee_count"),
                            avg("salary").alias("avg_salary"),
                            sum("salary").alias("total_salary")
                    )
                    .withColumn("budget_utilization", 
                            round(col("total_salary").divide(col("dept_budget")).multiply(100), 2))
                    .orderBy(desc("budget_utilization"));
            
            deptEfficiency.show();
            
        } catch (Exception e) {
            logger.error("Error calculating department statistics", e);
            throw e;
        }
    }
    
    /**
     * Perform complex analysis with custom SQL
     */
    public void performComplexAnalysis() {
        try {
            logger.info("Performing complex analysis...");
            
            // Register tables as temporary views
            readFullTable("employees").createOrReplaceTempView("employees");
            readFullTable("departments").createOrReplaceTempView("departments");
            
            // Complex SQL query
            String complexQuery = """
                SELECT 
                    d.name as department,
                    COUNT(e.id) as employee_count,
                    AVG(e.salary) as avg_salary,
                    STDDEV(e.salary) as salary_stddev,
                    d.budget,
                    ROUND((SUM(e.salary) / d.budget) * 100, 2) as budget_utilization,
                    CASE 
                        WHEN AVG(e.salary) > 80000 THEN 'High Pay'
                        WHEN AVG(e.salary) > 60000 THEN 'Medium Pay'
                        ELSE 'Low Pay'
                    END as pay_category
                FROM employees e
                JOIN departments d ON e.department_id = d.id
                GROUP BY d.id, d.name, d.budget
                ORDER BY avg_salary DESC
                """;
            
            Dataset<Row> complexResult = spark.sql(complexQuery);
            
            logger.info("=== Complex Analysis Results ===");
            complexResult.show();
            
            // Additional analysis - top performers
            String topPerformersQuery = """
                SELECT 
                    e.name as employee_name,
                    e.salary,
                    d.name as department,
                    RANK() OVER (PARTITION BY d.name ORDER BY e.salary DESC) as dept_rank,
                    RANK() OVER (ORDER BY e.salary DESC) as overall_rank
                FROM employees e
                JOIN departments d ON e.department_id = d.id
                """;
            
            Dataset<Row> topPerformers = spark.sql(topPerformersQuery);
            
            logger.info("=== Top Performers Analysis ===");
            topPerformers.filter(col("dept_rank").leq(3)).show();
            
        } catch (Exception e) {
            logger.error("Error performing complex analysis", e);
            throw e;
        }
    }
    
    /**
     * Read data using custom SQL query
     */
    public Dataset<Row> readWithCustomSQL() {
        try {
            logger.info("Reading data with custom SQL...");
            
            String customQuery = """
                (SELECT 
                    e.id,
                    e.name,
                    e.salary,
                    d.name as department_name,
                    CASE 
                        WHEN e.salary > 80000 THEN 'Senior'
                        WHEN e.salary > 60000 THEN 'Mid-level'
                        ELSE 'Junior'
                    END as level
                FROM employees e
                LEFT JOIN departments d ON e.department_id = d.id
                ORDER BY e.salary DESC) AS custom_query
                """;
            
            Dataset<Row> result = spark.read()
                    .jdbc(mysqlUrl, customQuery, connectionProps);
            
            logger.info("Custom SQL result - Row count: {}", result.count());
            result.show();
            
            return result;
            
        } catch (Exception e) {
            logger.error("Error reading with custom SQL", e);
            throw e;
        }
    }
} 