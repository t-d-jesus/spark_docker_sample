from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import min,max,row_number,first, col,desc
import datetime
from pathlib import Path
from pyspark.sql import functions as F
import json


def highest_weather_by_city(df_city_weathers):
    w = Window.partitionBy('name')

    return (
              df_city_weathers
                .select('*',row_number().over(w.orderBy(F.desc('temp_max') )).alias('rank') )
                .where('rank == 1')
            )
    
def avg_min_max_weather_by_city(df_city_weathers):
    return (
              df_city_weathers
                .groupBy('id','name')
                  .agg(
                      min('temp_min').alias('temp_min'),
                      max('temp_max').alias('temp_max')
                  )
                .withColumn('avg', ( F.col('temp_min') +F.col('temp_max'))  / 2)
            )

def avg_min_max_weather_by_city_day(df_city_weathers):
    return (
              df_city_weathers
                .groupBy('id','name')
                  .agg(
                      min('temp_min').alias('temp_min'),
                      max('temp_max').alias('temp_max')
                  )
                .withColumn('avg', ( F.col('temp_min') +F.col('temp_max'))  / 2)
            )

def write_data(df,mode,path):
    (
      df.write
        .mode(mode)
        .parquet(path)
    )

def get_logger(spark):
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(f'[{Path(__file__).stem}-{__name__}]: ')
    return LOGGER


def calculate_wheather_by_city(city):

    spark = SparkSession.builder.getOrCreate()

    spark.sparkContext.setLogLevel('INFO')

    LOGGER = get_logger(spark=spark)

    LOGGER.info('Set params')


    current_date = datetime.datetime.now()
    year = current_date.strftime("%Y")
    month = current_date.strftime("%m")
    day = current_date.strftime("%d")
    json_files = f"data/extract/{year}/{month}/*/{city}*"


    LOGGER.info(f'Read wheather from city {city}')
    df_cities = spark.read.json(json_files)


    LOGGER.info(f'Select fields and explode weather list') 
    df_city_weathers_exploded = df_cities.selectExpr('city.id',
                                                     'city.coord.lat', 
                                                     'city.coord.lon', 
                                                     'weather.dt',
                                                     'weather.dt_txt',
                                                     'city.name',
                                                     'explode(list) as weather')
    
    LOGGER.info(f'Select fields to be used in calculation') 

    df_city_weathers = df_city_weathers_exploded.selectExpr(
                                                            'id',
                                                            'name',
                                                            'lat',
                                                            'lon',
                                                            'dt',
                                                            'dt_txt',
                                                            'weather.main.temp_min',
                                                            'weather.main.temp_max'
                                                            )
    
    df_city_weathers.show()
 

    LOGGER.info(f'Calculate and save Highest Temperature by City and Month for city {city}') 

    HIGHEST_TEMPERATURE_BY_CITY_MONTH_PATH = f"data/output/HIGHEST_TEMPERATURE_BY_CITY_MONTH_PATH/{city}/{year}/{month}"

    df_highest_city_weather = (
                highest_weather_by_city(df_city_weathers)
                                      .select('id','name','dt','dt_txt','temp_max')
                                      .withColumn('year',  F.lit(year) )
                                      .withColumn('month',  F.lit(month) )
                            )

    
    # write_data(df_highest_city_weather,'overwrite',HIGHEST_TEMPERATURE_BY_CITY_MONTH_PATH)

    #TODO Não descomentar
    #Comments for discuss what should be the calculation logic
    # (
    #     df_city_weathers
    #         .groupBy('id','name')
    #           .agg(
    #               sum('temp_min').alias('temp_min_sum'),
    #               sum('temp_max').alias('temp_max_sum'),
    #               F.count('*').alias('count_temp'),
    #               min('temp_min').alias('temp_min'),
    #               max('temp_max').alias('temp_max')
    #           )
    #           .withColumn('avg', ( (F.col('temp_min_sum') / F.col('count_temp')   )+ (F.col('temp_max_sum') / F.col('count_temp'))  ) / 2  )
    #           .withColumn('avg2', (F.col('temp_min') +F.col('temp_max'))  / 2)
    #           .show(500)
    # )
    #TODO Não descomentar

    LOGGER.info(f'Calculate and save Avg, Min and MAx Wheather by day for city {city}') 

    AVG_MIN_MAX_BY_CITY_PATH = f"data/output/AVG_MIN_MAX_BY_CITY_PATH/{city}/{year}/{month}/{day}"

    df_avg_min_max_weather_by_city_month = (
                                avg_min_max_weather_by_city(df_city_weathers)
                                    .withColumn('year',  F.lit(year) )
                                    .withColumn('month', F.lit(month) )
                                    .withColumn('day',   F.lit(day) )
                            )

    # write_data(df_avg_min_max_weather_by_city_month,'overwrite',AVG_MIN_MAX_BY_CITY_PATH)



  
    dff = (
        df_city_weathers
            .withColumn('date',F.to_date('dt_txt'))
    )

    dff.show()
    dff.printSchema()


    (
        dff
        .orderBy('name',desc('temp_max'))
        .groupBy('name')
                  .agg(
                      min('temp_min').alias('temp_min'),
                      max('temp_max').alias('temp_max'),
                      first('date').alias('date')
                  )
                .withColumn('avg_temperature', ( F.col('temp_min') +F.col('temp_max'))  / 2)
                .orderBy(desc('date'))
        .show(5000)
    )


    # ( 
    #     df_city_weathers
    #     .orderBy(desc('temp_max'))
    #     .show()

    # )

    (
        df_cities
        .orderBy('city.name')
        .groupBy('city.name').count().show()
    )


    df_cities.show(500)


    spark.stop()



if __name__ == '__main__':

    cities = None
    with open('resources/city_list.json','r') as json_file:
      cities= json.load(json_file)


    # cities = ['Vue']
    for city in cities:
        print(f'Extract Data {city}')
        calculate_wheather_by_city(city)


    #generate mongo class
    #generate mongo class
    
    