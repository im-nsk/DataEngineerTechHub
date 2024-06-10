from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from datatime import datetime, timedelta
import random

spark = SparkSession.builder\
    .appName('Hude Example')\
    .config('spark.serializer', 'org.apche.spark.serializer.KryoSerializer')\
    .getOrCreate()


# ------- Ingest Real-Time Data -----------

def generate_transaction_data():
    return [
        (i, random.randint(1, 1000), random.randint(1, 500), random.uniform(10, 500), current_timestamp(), "completed") for i in range(1, 10001)
    ]

schema = ['transaction_id', 'customer_id', 'product_id', 'amount', 'timestamp', 'status']
transactions = spark.createDataFrame(generate_transaction_data(), schema=schema)

hudi_options = {
    'hoodie.table.name': 'transactions_hudi',
    'hoodie.datasource.write.recordkey.field': 'transaction_id',
    'hoodie.datasource.write.precombine.field': 'timestamp',
    'hoodie.datasource.write.table.name': 'transactions_hudi',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.hive.sync.enable': 'true',
    'hoodie.datasource.hive.sync.table': 'transactions_hudi'
}

transactions.write.format('hudi').options(**hudi_options).mode('append').save('/path/hudi/table')




# ------- Real-Time Data Updates -----------

updated_transactions = spark.createDataFrame([
    (500, 123, 456, 250.75, current_timestamp(), 'completed'),
    (750, 789, 123, 100.50, current_timestamp(), 'cancelled'),
], schema=schema)

updated_transactions.write.format('hudi').options(**hudi_options).mode('append').save('/path/hudi/table')




# ------- Increamental Processing -----------

beginning_time = (datetime.now() - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
incremental_df = spark.read.format('hudi').load('/path/hudi/table').filter(col('_hoodie_commit_time')> beginning_time)
incremental_df.show()




# ------- Efficient Querying -----------

spark.read.format('hudi').load('/path/hudi/table').createOrReplaceTempView('transactions_hudi_view')

spark.sql('''
            SELECT 
                customer_id, COUNT(*) as total_transactions, SUM(amount) as total_amount
            FROM transactions_hudi_view
          ''').show()







