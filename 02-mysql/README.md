# MySQL Operations Examples - Acceldata Data Platform

This repository contains comprehensive examples for MySQL database operations using Apache Spark across three different platforms: Java, Python, and Jupyter Notebooks. These examples are designed for application developers using the Acceldata data platform.

## Project Information

- **Company**: Acceldata Inc. (acceldata.io)
- **Group ID**: io.acceldata
- **Version**: 1.0.0
- **Purpose**: Educational examples for MySQL operations in data platforms

## Repository Structure

```
02-mysql/
├── spark-java/                    # Java examples with Gradle
│   ├── src/main/java/io/acceldata/examples/mysql/
│   │   ├── MySQLReadExample.java
│   │   ├── MySQLWriteExample.java
│   │   └── MySQLOperationsExample.java
│   ├── build.gradle
│   └── README.md
├── spark-python/                  # Python examples
│   ├── mysql_read_example.py
│   ├── mysql_write_example.py
│   ├── mysql_operations_example.py
│   ├── requirements.txt
│   └── README.md
├── jupyter-notebook/              # Jupyter notebook examples
│   ├── MySQL_Operations_Example.ipynb
│   ├── requirements.txt
│   └── README.md
└── README.md                      # This file
```

## Prerequisites

- **Java 11+** (for Java examples)
- **Python 3.8+** (for Python and Jupyter examples)
- **Apache Spark 3.5.0**
- **MySQL 8.0+** (accessible at mysql://mysql-server:3306)
- **MySQL JDBC Driver 8.0.33**
- **Database credentials** (username/password or connection string)
- **Docker & Kubernetes** (for containerized Java examples)

## Quick Start

### 1. Java Examples (Containerized Deployment)

```bash
cd spark-java

# Build the project and Docker image
./gradlew build
./gradlew buildDockerImage

# For local development/testing
./gradlew runMySQLOperations
./gradlew runMySQLRead
./gradlew runMySQLWrite

# Deploy to Kubernetes (handled by Acceldata platform)
# The platform will automatically mount:
# - Database connection secrets
# - SSL certificates if required
```

### 2. Python Examples

```bash
cd spark-python

# Install dependencies
pip install -r requirements.txt

# Run comprehensive example
python mysql_operations_example.py

# Run specific examples
python mysql_read_example.py
python mysql_write_example.py
```

### 3. Jupyter Notebook

```bash
cd jupyter-notebook

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Lab
jupyter lab

# Open MySQL_Operations_Example.ipynb
```

## Examples Overview

### Common Features Across All Platforms

1. **Read Operations**
   - Full table reads
   - Query-based reads with WHERE clauses
   - Partitioned reads for large tables
   - Custom SQL queries
   - JOIN operations across tables

2. **Write Operations**
   - DataFrame to table (append/overwrite)
   - Batch inserts with optimal partitioning
   - Upsert operations (INSERT ON DUPLICATE KEY UPDATE)
   - Transactional writes
   - Bulk data loading

3. **Advanced Operations**
   - Connection pooling
   - SSL/TLS connections
   - Performance optimization
   - Error handling and retry logic
   - Data type mapping

### Platform-Specific Features

#### Java (Gradle + Docker)
- Enterprise-grade connection management
- Containerized deployment for Kubernetes
- Secure credential handling
- Connection pooling with HikariCP
- Comprehensive error handling and logging
- Fat JAR creation for Spark submission
- Docker image building and deployment

#### Python
- Object-oriented design with classes
- Type hints for better code documentation
- SQLAlchemy integration for advanced operations
- Comprehensive logging
- Modular structure for reusability

#### Jupyter Notebook
- Interactive data exploration
- Step-by-step explanations
- Data visualization capabilities
- Educational format with markdown documentation

## Configuration

### MySQL Setup

Ensure your MySQL server is running and accessible:

```sql
-- Create database and user
CREATE DATABASE acceldata_examples;
CREATE USER 'spark_user'@'%' IDENTIFIED BY 'spark_password';
GRANT ALL PRIVILEGES ON acceldata_examples.* TO 'spark_user'@'%';
FLUSH PRIVILEGES;

-- Create sample tables
USE acceldata_examples;
CREATE TABLE employees (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2),
    hire_date DATE
);

CREATE TABLE departments (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50),
    budget DECIMAL(12,2)
);
```

### Spark Configuration

All examples include optimized Spark configurations for MySQL:

```
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.execution.arrow.pyspark.enabled=true
```

## Data Operations Supported

| Operation | Read | Write | Batch | Streaming | Best Use Case |
|-----------|------|-------|-------|-----------|---------------|
| Full Table | ✅ | ✅ | ✅ | ❌ | Small to medium tables |
| Partitioned | ✅ | ✅ | ✅ | ❌ | Large tables |
| Query-based | ✅ | ❌ | ✅ | ❌ | Filtered data |
| Upsert | ❌ | ✅ | ✅ | ❌ | Data synchronization |
| Transactional | ✅ | ✅ | ✅ | ❌ | ACID compliance |

## Performance Best Practices

### 1. Connection Management
- Use connection pooling
- Configure appropriate timeout values
- Limit concurrent connections

### 2. Partitioning Strategy
- Partition large tables by numeric columns
- Use `numPartitions` parameter wisely
- Consider `partitionColumn`, `lowerBound`, `upperBound`

### 3. Query Optimization
- Use appropriate WHERE clauses
- Create indexes on frequently queried columns
- Avoid SELECT * for large tables

### 4. Batch Operations
- Use batch inserts for better performance
- Configure `batchsize` parameter
- Consider using `truncate` mode for overwrites

## Spark Submit Examples

### Local Mode
```bash
spark-submit \
  --class io.acceldata.examples.mysql.MySQLOperationsExample \
  --master local[*] \
  --jars mysql-connector-java-8.0.33.jar \
  spark-java/build/libs/mysql-operations-examples-fat-1.0.0.jar
```

### Cluster Mode
```bash
spark-submit \
  --class io.acceldata.examples.mysql.MySQLOperationsExample \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  --jars mysql-connector-java-8.0.33.jar \
  spark-java/build/libs/mysql-operations-examples-fat-1.0.0.jar
```

## Connection String Examples

### Standard Connection
```
jdbc:mysql://localhost:3306/acceldata_examples?user=spark_user&password=spark_password
```

### SSL Connection
```
jdbc:mysql://localhost:3306/acceldata_examples?user=spark_user&password=spark_password&useSSL=true&requireSSL=true
```

### Connection with Timezone
```
jdbc:mysql://localhost:3306/acceldata_examples?user=spark_user&password=spark_password&serverTimezone=UTC
```

## Troubleshooting

### Common Issues

1. **Connection Timeout**
   ```bash
   # Increase connection timeout
   --conf spark.sql.execution.arrow.maxRecordsPerBatch=1000
   ```

2. **Memory Issues**
   ```bash
   # Adjust partition size
   --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB
   ```

3. **SSL Certificate Issues**
   ```bash
   # Add SSL certificate to truststore
   keytool -import -alias mysql -file mysql-cert.pem -keystore truststore.jks
   ```

## Security Best Practices

### 1. Credential Management
- Use environment variables for credentials
- Implement secret management systems
- Rotate passwords regularly

### 2. Network Security
- Use SSL/TLS connections
- Implement firewall rules
- Use VPN for remote connections

### 3. Access Control
- Follow principle of least privilege
- Use dedicated service accounts
- Implement role-based access control

## Development Guidelines

### Code Standards
- Follow language-specific conventions
- Include comprehensive error handling
- Add logging for debugging and monitoring
- Document all public methods and classes

### Testing
- Test with different MySQL versions
- Validate data integrity after operations
- Test with various data sizes
- Include edge case handling

### Documentation
- Keep README files updated
- Include code comments for complex logic
- Provide usage examples
- Document configuration requirements

## Use Cases

These examples are suitable for:

1. **Data Engineers**: Learning MySQL integration patterns
2. **Application Developers**: Understanding database connectivity
3. **Data Scientists**: Accessing relational data for analysis
4. **DevOps Teams**: Deploying data processing applications
5. **Students**: Learning database integration with Spark

## Integration with Acceldata Platform

These examples demonstrate patterns commonly used in:

- **Data Ingestion from OLTP Systems**
- **ETL/ELT Processes**
- **Data Warehouse Loading**
- **Real-time Analytics**
- **Data Quality Checks**
- **Monitoring and Observability**

## Contributing

We welcome contributions to improve these examples:

1. Fork the repository
2. Create a feature branch
3. Add your improvements
4. Test thoroughly
5. Submit a pull request

### Contribution Guidelines
- Follow existing code structure
- Add appropriate tests
- Update documentation
- Ensure compatibility across platforms

## License

Copyright © 2024 Acceldata Inc. All rights reserved.

These examples are provided for educational purposes as part of the Acceldata data platform documentation.

## Support and Resources

### Support
- **Website**: https://acceldata.io
- **Email**: support@acceldata.io
- **Documentation**: [Acceldata Platform Docs]

### Additional Resources
- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [MySQL Documentation](https://dev.mysql.com/doc/)
- [Spark JDBC Documentation](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

## Roadmap

Future enhancements planned:
- [ ] MySQL 8.0 specific features
- [ ] Streaming data examples
- [ ] Advanced security configurations
- [ ] Performance benchmarking tools
- [ ] Connection pooling examples
- [ ] Sharding and partitioning strategies

---

**Built with ❤️ by the Acceldata Platform Team** 