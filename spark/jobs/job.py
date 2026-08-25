import os

from pyspark.sql import SparkSession

JDBC_URL = os.getenv(
    "SOURCE_DB_JDBC_URL",
    "jdbc:sqlserver://;serverName=45.124.94.158;databaseName=xomdata_dataset",
)
DB_USER = os.getenv("SOURCE_DB_USER", "andmse182449")
DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD", "QpJ6Czdp4%ljHA")
DB_DRIVER = os.getenv(
    "SOURCE_DB_DRIVER", "com.microsoft.sqlserver.jdbc.SQLServerDriver"
)
TABLE_NAME = "vietnam_ecommerce.shopee_orders"
ROW_LIMIT = int(os.getenv("SOURCE_ROW_LIMIT", "100"))

POLARIS_URI = os.getenv("POLARIS_URI", "http://polaris:8181/api/catalog")
POLARIS_CATALOG = os.getenv("POLARIS_CATALOG", "lakehouse")
POLARIS_CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "root")
POLARIS_CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "polarisadmin")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

TARGET_TABLE = "polaris.bronze.shopee_orders"

def main():
    spark = (
        SparkSession.builder
        .appName("ReadDBToSpark")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", POLARIS_URI)
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
        .config(
            "spark.sql.catalog.polaris.credential",
            f"{POLARIS_CLIENT_ID}:{POLARIS_CLIENT_SECRET}",
        )
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.rest-metrics-reporting-enabled", "false")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.polaris.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
        .config("spark.sql.catalog.polaris.s3.access-key-id", MINIO_ACCESS_KEY)
        .config("spark.sql.catalog.polaris.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.sql.catalog.polaris.s3.region", MINIO_REGION)
        .getOrCreate()
    )

    # Limit inside SQL Server so Spark never transfers the full source table.
    source_query = f"(SELECT TOP ({ROW_LIMIT}) * FROM {TABLE_NAME}) AS source_rows"
    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", source_query)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", DB_DRIVER)
        .option("encrypt", "true")
        .option("trustServerCertificate", "true")
        .load()
        .cache()
    )

    source_count = df.count()
    print(f"Số dòng đọc được: {source_count}")
    df.printSchema()
    df.show(10, truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.bronze")
    df.writeTo(TARGET_TABLE).using("iceberg").createOrReplace()
    written_count = spark.table(TARGET_TABLE).count()
    print(f"Đã ghi và đọc lại {written_count} dòng từ {TARGET_TABLE} trên MinIO")
    df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
