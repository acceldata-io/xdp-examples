# Hive Operations Examples - Acceldata Data Platform

This repository contains comprehensive examples for Hive operations using Apache Spark across three different platforms: Java, Python, and Jupyter Notebooks. These examples are designed for application developers using the Acceldata data platform.

## Project Information

- **Company**: Acceldata Inc. (acceldata.io)
- **Group ID**: io.acceldata
- **Version**: 1.0.0
- **Purpose**: Educational examples for Hive operations in data platforms

## Repository Structure

```
02-hive-operations/
├── spark-java/                    # Java examples with Gradle
│   ├── src/main/java/io/acceldata/examples/hive/
│   │   ├── HiveReadExample.java
│   │   ├── HiveWriteExample.java
│   │   └── HiveOperationsExample.java
│   ├── build.gradle
│   └── README.md
├── spark-python/                  # Python examples
│   ├── hive_operations_example.py
│   ├── requirements.txt
│   └── README.md
├── jupyter-notebook/              # Jupyter notebook examples
│   ├── Hive_Operations_Example.ipynb
│   ├── requirements.txt
│   └── README.md
└── README.md                      # This file
```

## Prerequisites

- **Java 11+** (for Java examples)
- **Python 3.8+** (for Python and Jupyter examples)
- **Apache Spark 3.5.0**
- **Hadoop 3.3.4**
- **Hive Metastore** (accessible from Spark)
- **Kerberos authentication** (for containerized deployment)
- **Docker & Kubernetes** (for containerized Java examples)

## Quick Start

### 1. Java Examples (Containerized Deployment)

```bash
cd spark-java

# Build the project and Docker image
./gradlew build
./gradlew buildDockerImage

# For local development/testing
./gradlew runHiveOperations
./gradlew runHiveRead
./gradlew runHiveWrite

# Deploy to Kubernetes (handled by Acceldata platform)
# The platform will automatically mount:
# - /etc/krb5.conf
# - /etc/hadoop/conf/*.xml
# - /etc/user.keytab
```

### 2. Python Examples

```bash
cd spark-python

# Install dependencies
pip install -r requirements.txt

# Run comprehensive example
python hive_operations_example.py
```

### 3. Jupyter Notebook

```bash
cd jupyter-notebook

# Install dependencies
pip install -r requirements.txt

# Start Jupyter Lab
jupyter lab

# Open Hive_Operations_Example.ipynb
```

## Examples Overview

### Common Features Across All Platforms

1. **Database Operations**
   - Create database if not exists
   - Create table if not exists
   - Insert rows
   - Read rows
   - Delete table

2. **Advanced Operations**
   - Data analysis and transformations
   - JOIN operations
   - Aggregations and reporting
   - Performance optimizations

### Platform-Specific Features

#### Java (Gradle + Docker)
- Enterprise-grade structure with proper packaging
- Containerized deployment for Kubernetes
- Kerberos authentication with keytab
- Automatic Hive configuration loading
- Comprehensive error handling and logging
- Fat JAR creation for Spark submission
- Docker image building and deployment

#### Python
- Object-oriented design with classes
- Type hints for better code documentation
- Comprehensive logging
- Modular structure for reusability

#### Jupyter Notebook
- Interactive data exploration
- Step-by-step explanations
- Visualization capabilities
- Educational format with markdown documentation

## Configuration

### Hive Setup

Ensure your Hive Metastore is running and accessible from Spark.

### Spark Configuration

All examples rely on Spark Operator or external configuration for Spark settings.

## Performance Best Practices

- Use partitioned tables for large datasets
- Use Parquet format for analytics workloads
- Cache frequently accessed DataFrames
- Unpersist when no longer needed

## Development Guidelines

- Follow language-specific conventions (Java: camelCase, Python: snake_case)
- Include comprehensive error handling
- Add logging for debugging and monitoring
- Document all public methods and classes

## Use Cases

These examples are suitable for:

1. **Data Engineers**: Learning Hive integration patterns
2. **Application Developers**: Understanding data platform APIs
3. **Data Scientists**: Accessing data for analysis
4. **DevOps Teams**: Deploying data processing applications
5. **Students**: Learning big data technologies

## Integration with Acceldata Platform

These examples demonstrate patterns commonly used in:

- **Data Ingestion Pipelines**
- **ETL/ELT Processes**
- **Data Lake Operations**
- **Analytics Workloads**
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
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Hive Documentation](https://cwiki.apache.org/confluence/display/Hive/Home)
- [Acceldata Platform Overview](https://acceldata.io/platform)

---

**Built with ❤️ by the Acceldata Platform Team** 