from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pytz

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ColabSparkSession")
    .master("local[*]")
    .getOrCreate()
)

spark

# it represents the current date
local_tz = pytz.timezone('Asia/Kolkata')
date_key = datetime.now(local_tz)
formatted_date = date_key.strftime("%d/%m/%Y")
print(formatted_date)


from google.colab import drive
drive.mount('/content/drive')

def create_my_sql_data_frame(dbname, table_name, environment, spark):
    """create spark dataframe from mysql"""
    data_frame = None
    mysql_hostname = 'db.pre-prod.flipkart.com'
    mysql_jdbcPort = 3306
    mysql_dbname = dbname
    mysql_username = 'root'
    mysql_password = 'my_pass'

    if environment == 'prod':
        mysql_hostname = 'db.pre.flipkart.com'
        mysql_jdbcPort = 3306
        mysql_dbname = dbname
        mysql_username = 'dev'
        mysql_password = 'pass_2'

    mysql_connectionProperties = {
        "user": mysql_username,
        "password": mysql_password,
        "driver": "com.mysql.jdbc.Driver"
    }

    mysql_jdbc_url = "jdbc:mysql://{0}:{1}/{2}?DateTimeBehavior".format(mysql_hostname,
                                                                                          mysql_jdbcPort,
                                                                                          mysql_dbname)
    data_frame = spark.read.jdbc(url=mysql_jdbc_url, table=table_name, properties=mysql_connectionProperties)
    return data_frame

    # Install gdown if not already installed
#!pip install gdown
import gdown

# Extract the file ID from your Google Drive link
file_id = '1BRWTl2AaxwuBp5la_I3y19Mz4q4ZUQSc'
output_file = 'customers.csv'

# Download the file to the local Colab environment
gdown.download(id=file_id, output=output_file, quiet=False)

# Now, read the local CSV file using Spark
df_sales = spark.read.csv(output_file, header=True, inferSchema=True)
df_sales.printSchema()



df_sales_final=df_sales.select("Region",
                              "total_revenue",
                               "total_profit","unit_price")
df_sales_final.show()
df_sales_final.count()


df_sales_final.write.mode('overwrite').parquet(f"/Volumes/workspace/ds_27/batch_27/{formatted_date}")

