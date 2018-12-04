# Databricks notebook source
# MAGIC %md
# MAGIC #### Flow Design:
# MAGIC 1. Use the function of loading historical data.
# MAGIC 3. Read archived data in JSON format into a DataFrame.
# MAGIC 4. Perform transformation if necessary.
# MAGIC 5. Write the final DataFrame into the table in Redshift.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Functions
# MAGIC 1. `map_paths_with_target_tables(project, queue_name, start_date, end_date, db_name, sql_query)`
# MAGIC 2. `load_transform_write(tablePathMap, latestSchema, writeMode)`

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import to_json, explode, upper
import pprint

'''Functions'''
def map_paths_with_target_tables(project, queue_name, start_date, end_date, db_name, sql_query):
  table_path_map = {}
  for (q,s) in zip(queue_name, sql_query): #Use Zip() to loop two lists in parallel.
    if "sprocket" not in q:
      table_name = "sprocket_" + q
    else:
      table_name = q

    full_table_name = db_name+table_name
    dateBounds = [start_date, end_date]
    #Convert the datetime STRING in the `dateBounds` to DATETIME type for the datetime operation of the next step.
    start, end = [datetime.strptime(_,"%Y-%m-%d") for _ in dateBounds]
    # Generate a list of tuples that consist of (year,month, day) of the date.
    date_tuples = [((start + timedelta(days=x)).strftime("%Y"),(start + timedelta(days=x)).strftime("%m"),(start + timedelta(days=x)).strftime("%d")) for x in range((end - start).days)]
    ymd_part_of_dir = ["/yr=%s/mo=%s/day=%s/hr=*" % (t[0], t[1], t[2]) for t in date_tuples]  
    table_path = {full_table_name:([(project+q+ymd) for ymd in ymd_part_of_dir],s)}
    table_path_map.update(table_path)

  return table_path_map

def load_transform_write(tablePathMap, latestSchema, writeMode):
  for k, v in table_path_map.items():
    try:
      '''Load into a dataframe'''
      targetTable = k
      pathForLoad = v[0]
      sqlQuery = v[1]
      df = sqlContext.read.json(pathForLoad, schema=latestSchema)
      df.registerTempTable("dataTable")
      #Read into a Spark DataFrame.
      df = sqlContext.sql(sqlQuery)

      '''Credentials'''
      # Configure a S3 bucket to hold temporary files
      tempDir = "s3n://cscranalytics-dev-spark-redshift/"
      #Configure Redshift credentials
      jdbcUsername = "oobe_user"
      jdbcPassword = "Pt18hVoulgYr0JjpKyzG"
      jdbcHostname = "datalake.cbvz4ktkbd57.us-west-2.redshift.amazonaws.com"
      jdbcPort = 5439
      jdbcDatabase = "snowplow"
      jdbcUrl = r"jdbc:redshift://%s:%s/%s?user=%s&password=%s" %(jdbcHostname,jdbcPort,jdbcDatabase,jdbcUsername,jdbcPassword)

      '''Write'''
      df.write\
        .format("com.databricks.spark.redshift")\
        .option("aws_iam_role", "arn:aws:iam::841913685921:role/cscranalytics-dev-redshiftcopyunload-role")\
        .option("autoenablessl", "false")\
        .option("url", jdbcUrl)\
        .option("dbtable", targetTable)\
        .option("tempdir", tempDir)\
        .mode(writeMode)\
        .save()
    
      print('''"{}" were {}ed into "{}"'''.format(pathForLoad, writeMode, targetTable))
    
    except Exception as e:
      print(e)



# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup Parameters

# COMMAND ----------

# Source and Target Information
pjt = "/mnt/sprocket-prod/"
db = "mobile_app_prod."
q = ['sprocket_experiment_assignments']
"""Specify the data path(s) of the date/date range of interest."""
start_dt = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
end_dt = datetime.utcnow().strftime("%Y-%m-%d") #the end date won't be included in the final list


# COMMAND ----------

#SQL queries
sprocket_experiment_assignments = """
select
service.oid as server_oid,
body.app_session_uuid,
cast(service.server_timestamp/1000 as Timestamp) as utc_timestamp,
service.server_timestamp,
body.device_id,
body.os_type,
body.version as app_version,
body.experiment_id,
body.variant_id,
body.language_code,
body.country_code,
body.device_brand,
body.device_type,
body.manufacturer,
body.os_version,
body.product_id,
body.product_name,
body.wifi_ssid,
geo.accuracy_radius as geo_accuracy_radius,
geo.city as geo_city,
geo.continent_code as geo_continent_code,
geo.country_code as geo_country_code,
geo.ip_connection as geo_ip_connection,
geo.ip_domain as geo_ip_domain,
geo.ip_isp as geo_ip_isp,
geo.ip_org as geo_ip_org,
geo.latitude as geo_latitude,
geo.longitude as geo_longitude,
geo.metro_code as geo_metro_code,
geo.postal_code as geo_postal_code,
geo.region as geo_region,
geo.region_name as geo_region_name,
geo.source as geo_source,
geo.timezone as geo_timezone,
body.timezone_description,
body.timezone_offset_seconds
from dataTable
where body.experiment_id <> 100
"""

sql_query = [sprocket_experiment_assignments]


# COMMAND ----------

#Schema in Use.
exp_assignments_schema_20181115 = sqlContext.read.json("/mnt/sprocket-prod/sprocket_experiment_assignments/yr=2018/mo=11/day=15/hr=*").schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Take Action!

# COMMAND ----------

table_path_map = map_paths_with_target_tables(pjt,q,start_dt,end_dt,db,sql_query)
load_transform_write(table_path_map,exp_assignments_schema_20181115, 'append')
