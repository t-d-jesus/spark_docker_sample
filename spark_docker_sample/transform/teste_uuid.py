from time_uuid import TimeUUID
from uuid import UUID
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf


# 02fa04b4-d8a0-11ed-dc9e-489b1b483802
# 38f4d5da-d8a0-11ed-a218-22f30005fd93
# 49d0c8b4-d8a0-11ed-f04f-7bc78f513cde

def get_spark_session():
    return SparkSession.builder.getOrCreate()

@udf(returnType=TimestampType())
def timeuuid_to_datetime(time_uuid):
    uuid = UUID(time_uuid)
    timeuuid = TimeUUID(bytes=uuid.bytes)
    return timeuuid.get_datetime()

if __name__ == '__main__':
    print('teste')

    spark = get_spark_session()

    data = [
        ('1', 'user1', '02fa04b4-d8a0-11ed-dc9e-489b1b483802'),
        ('2', 'user2', '38f4d5da-d8a0-11ed-a218-22f30005fd93'),
        ('3', 'user3', '49d0c8b4-d8a0-11ed-f04f-7bc78f513cde')
    ]

    columns = ['id', 'user', 'timeuuid']

    df = spark.createDataFrame(data,columns)

    df.show()

    df.withColumn('uuid_datetime', timeuuid_to_datetime('timeuuid')) .show(truncate = False)


    spark.stop()