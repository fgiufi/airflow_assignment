from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import expr, trunc, col, datediff
from pyspark.sql.functions import sum
from pyspark.sql.functions import lit

file_path = "/Users/shiyu.chenthoughtworks.com/github/scalatest/scalatest01/src/data/"


def import_postgres_jar():
    postgres_jar_path = "/Users/shiyu.chenthoughtworks.com/airflow/dags/postgresql-42.5.0.jar"
    master = "local"
    conf = SparkConf() \
        .setMaster(master) \
        .set("spark.driver.extraClassPath", postgres_jar_path)
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = SQLContext(sc)
    spark = sqlContext.sparkSession
    return spark


def read_parquet_file(parquet_file_path, *table_names):
    df_list = []
    for df_name in table_names:
        df = import_postgres_jar().read.parquet(
            file_path + parquet_file_path + df_name)
        df_list.append(df)
    return df_list


def write_to_postgresql(df, table_name):
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://localhost:54321/report") \
        .option("dbtable", f"report." + table_name) \
        .option("user", "shiyu") \
        .option("password", "123456") \
        .save()


def read_csv(spark, path):
    sales_order_data_csv = spark.read.csv(
        path=path,
        header=True,
    )
    return sales_order_data_csv


def change_schema_name_datatype(df, schema):
    df = df.toDF(*(schema.fieldNames())).selectExpr(
        *[f"CAST ({c.name} AS {c.dataType.simpleString()}) {c.name}" for c in schema])
    return df


def write_parquet(df, parquet_path, file_name):
    df.write.option("header", "true").mode("overwrite").parquet(
        file_path + parquet_path + file_name)


class DataProcessing:

    @staticmethod
    def print_parquet_massage():
        print("convert csv to parquet!")

    @staticmethod
    def print_data_processing_massage():
        print("Start data processing！！")

    @staticmethod
    def convert_csv_to_parquet_product_data(**kwargs):
        product_raw_path = kwargs['raw_path']
        product_parquet_path = kwargs['parquet_path']
        product_data_file_name = file_path + product_raw_path + "ProductData.csv"
        product_csv = read_csv(import_postgres_jar(), product_data_file_name)
        schema = StructType([StructField("product_id", IntegerType()),
                             StructField("name", StringType()),
                             StructField("product_number", StringType()),
                             StructField("color", StringType()),
                             StructField("standard_cost", DoubleType()),
                             StructField("list_price", DoubleType()),
                             StructField("size", StringType()),
                             StructField("weight", DoubleType()),
                             StructField("product_category_id", IntegerType()),
                             StructField("product_model_id", IntegerType()),
                             StructField("sell_start_date", StringType()),
                             StructField("sell_end_date", StringType()),
                             StructField("discontinue_date", StringType()),
                             StructField("thumb_nail_photo", StringType()),
                             StructField("thumb_nail_photo_file_name", StringType()),
                             StructField("rowguid", StringType()),
                             StructField("modified_date", StringType())])
        df = change_schema_name_datatype(product_csv, schema)
        write_parquet(df, product_parquet_path, "product_parquet")

    @staticmethod
    def convert_csv_to_parquet_sales_order_data(**kwargs):
        sales_raw_path = kwargs['raw_path']
        sales_parquet_path = kwargs['parquet_path']
        sales_order_data_file_name = file_path + sales_raw_path + "SalesOrderData.csv"
        sales_order_data_csv = read_csv(import_postgres_jar(), sales_order_data_file_name)
        schema = StructType([StructField("sales_order_id", IntegerType()),
                             StructField("sales_order_detail_id", IntegerType()),
                             StructField("revision_number", IntegerType()),
                             StructField("order_date", StringType()),
                             StructField("due_date", StringType()),
                             StructField("ship_date", StringType()),
                             StructField("status", IntegerType()),
                             StructField("online_order_flag", IntegerType()),
                             StructField("sales_order_number", StringType()),
                             StructField("purchase_order_number", StringType()),
                             StructField("account_number", StringType()),
                             StructField("customer_id", IntegerType()),
                             StructField("ship_to_address_id", IntegerType()),
                             StructField("bill_to_address_id", IntegerType()),
                             StructField("ship_method", StringType()),
                             StructField("credit_card_approval_code", StringType()),
                             StructField("sub_total", DoubleType()),
                             StructField("tax_amt", DoubleType()),
                             StructField("freight", DoubleType()),
                             StructField("total_due", DoubleType()),
                             StructField("comment", StringType()),
                             StructField("order_gty", IntegerType()),
                             StructField("product_id", IntegerType()),
                             StructField("unit_price", DoubleType()),
                             StructField("unit_price_discount", DoubleType()),
                             StructField("line_total", DoubleType()),
                             StructField("rowguid", StringType()),
                             StructField("modified_date", StringType())
                             ])
        df = change_schema_name_datatype(sales_order_data_csv, schema)
        write_parquet(df, sales_parquet_path, "sales_order_parquet")

    @staticmethod
    def convert_csv_to_parquet_customer_address(**kwargs):
        customer_raw_path = kwargs['raw_path']
        customer_parquet_path = kwargs['parquet_path']
        customer_address_file_name = file_path + customer_raw_path + "CustomerAddress-data.csv"
        customer_address_csv = read_csv(import_postgres_jar(), customer_address_file_name)
        schema = StructType([StructField("customer_id", IntegerType()),
                             StructField("address_id", IntegerType()),
                             StructField("address_type", StringType()),
                             StructField("rowguid", StringType()),
                             StructField("modified_date", StringType())
                             ])
        df = change_schema_name_datatype(customer_address_csv, schema)
        write_parquet(df, customer_parquet_path, "customer_address_parquet")

    @staticmethod
    def convert_csv_to_parquet_address_data(**kwargs):
        address_raw_path = kwargs['raw_path']
        address_parquet_path = kwargs['parquet_path']
        address_data_file_name = file_path + address_raw_path + "AddressData.csv"
        address_data_csv = read_csv(import_postgres_jar(), address_data_file_name)
        schema = StructType([StructField("address_id", IntegerType()),
                             StructField("address_line1", StringType()),
                             StructField("address_line2", StringType()),
                             StructField("city", StringType()),
                             StructField("status_province", StringType()),
                             StructField("country_region", StringType()),
                             StructField("postal_code", StringType()),
                             StructField("rowguid", StringType()),
                             StructField("modified_date", StringType())])
        df = change_schema_name_datatype(address_data_csv, schema)
        write_parquet(df, address_parquet_path, "address_data_parquet")

    @staticmethod
    def sales_profit_diff_cities(**kwargs):
        execution_date = kwargs['execution_date']
        parquet_path = kwargs['parquet_path']
        table_names = ["product_parquet", "salesOrder_parquet", "customer_address_parquet", "address_data_parquet"]

        product_data, sales_data, customer_data, address_data = read_parquet_file(parquet_path, *table_names)
        join_table_prod_and_sales = sales_data.join(
            product_data,
            "product_id"
        )
        join_three_table = join_table_prod_and_sales.join(
            customer_data,
            "customer_id"
        )
        join_four_table = join_three_table.join(
            address_data,
            "address_id"
        )

        join_three_table_profits = join_four_table.withColumn("profits", expr("unit_price*order_gty-standard_cost"))
        order_date_month = join_three_table_profits.withColumn("order_date_month", trunc(col("order_date"), "month"))
        sum_due_and_profits = order_date_month.withColumn("execution_date", lit(execution_date))
        sum_due_and_profits = sum_due_and_profits\
            .groupBy("city", "order_date_month") \
            .agg(sum("total_due").alias("sum_total_due"),
                 sum("profits").alias("sum_profits"))\
            .sort("sum_total_due")

        write_to_postgresql(sum_due_and_profits, "sales_profit_diff_cities")

    @staticmethod
    def total_profit_top10(**kwargs):
        execution_date = kwargs['execution_date']
        parquet_path = kwargs['parquet_path']
        table_names = ["product_parquet", "salesOrder_parquet"]

        product_data, sales_data = read_parquet_file(parquet_path, *table_names)
        join_sales_and_product = sales_data.join(
            product_data,
            "product_id"
        )
        profits = join_sales_and_product.withColumn("profits", expr("unit_price*order_gty-standard_cost"))
        # .sort(col("profits").desc)
        total_profit_top_10 = profits.withColumn("execution_date", lit(execution_date))
        total_profit_top_10 = total_profit_top_10.limit(10).select(col("product_id"), col("profits"))
        write_to_postgresql(total_profit_top_10, "total_profit_top10")

    @staticmethod
    def Longest_time_span(**kwargs):
        execution_date = kwargs['execution_date']
        parquet_path = kwargs['parquet_path']
        table_names = ["product_parquet", "salesOrder_parquet"]

        product_data, sales_data = read_parquet_file(parquet_path, *table_names)
        join_sales_and_product_table = sales_data.alias("sales").join(
            product_data.alias("product"),
            "product_id")
        longest_time_span = join_sales_and_product_table.select(col("sales_order_detail_id"),
                                                                col("product.modified_date"),
                                                                col("order_date"),
                                                                datediff(col("order_date"),
                                                                         col("product.modified_date")).alias(
                                                                    "date_diff")).limit(50)  # .sort(col("date_diff").desc)
        longest_time_span = longest_time_span.withColumn("execution_date", lit(execution_date))
        write_to_postgresql(longest_time_span, "longest_time_span")
