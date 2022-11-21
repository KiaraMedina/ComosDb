# Databricks notebook source
import azure.cosmos.cosmos_client as cosmos_client
from azure.cosmos import CosmosClient, PartitionKey, exceptions

# COMMAND ----------

cosmosUri = 'https://comosvalidate.documents.azure.com:443/'
pKey = 'W9EwtdYrTmo6Xra8Qb185JdFriNv4xjIe4UVoG8DJuxWtwcVXs0R3kGwXzmcEzo3BtgTLYULFCjvACDb1SWRWA=='
client = cosmos_client.CosmosClient(cosmosUri, {'masterKey': pKey})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creacion de base de datos y container

# COMMAND ----------

# 1. create database 
newDatabaseName = 'database-v2'
 
newDatabase = client.create_database(newDatabaseName)
print('\n1. Database created with name: ', newDatabase.id)

# COMMAND ----------

# 2. Get Database properties
dbClient = client.get_database_client(newDatabaseName)

dbProperties = dbClient.read()
print('\n2. DB Properties: ', dbProperties)

# COMMAND ----------

# 3. Create a new Container
newContainerName = 'PersonContainer'
newContainer = dbClient.create_container(id=newContainerName,partition_key=PartitionKey(path="/id"))
print('\n3. Container created with name: ', newContainer.id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Crear el dataframe

# COMMAND ----------

# 6. CrearSchema 

# COMMAND ----------

# 5. Create DataFrame
fakeFriendsDf = spark.read\
.format("csv")\
.option("header",True)\
.option("inferSchema",True)\
.load("/FileStore/tables/fakefriends.csv")
fakeFriendsDf.printSchema()

# COMMAND ----------

fakeFriendsDf.createOrReplaceTempView('v_fakeFriends')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table tb_fakeFriends
# MAGIC as
# MAGIC select * from v_fakeFriends where id !=0 limit 11

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tb_fakeFriends

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %scala
# MAGIC val fakeFriendsDfLimit = spark.read.table("tb_fakeFriends")

# COMMAND ----------

# MAGIC %scala
# MAGIC fakeFriendsDfLimit.count()

# COMMAND ----------

fakeFriendsDfLimit.printSchema()

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
# MAGIC val fakeFriendsDfLimit_Cast = fakeFriendsDfLimit.withColumn("id", $"id".cast(StringType))

# COMMAND ----------

# MAGIC %scala
# MAGIC fakeFriendsDfLimit_Cast.printSchema()

# COMMAND ----------

# MAGIC %scala
# MAGIC fakeFriendsDfLimit_Cast.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save to CosmosDb

# COMMAND ----------

# MAGIC %scala
# MAGIC val cosmosEndpoint = "https://comosvalidate.documents.azure.com:443/"
# MAGIC val cosmosMasterKey = "W9EwtdYrTmo6Xra8Qb185JdFriNv4xjIe4UVoG8DJuxWtwcVXs0R3kGwXzmcEzo3BtgTLYULFCjvACDb1SWRWA=="
# MAGIC val cosmosDatabaseName = "database-v2"
# MAGIC val cosmosContainerName = "PersonContainer"
# MAGIC 
# MAGIC val cfg = Map("spark.cosmos.accountEndpoint" -> cosmosEndpoint,
# MAGIC   "spark.cosmos.accountKey" -> cosmosMasterKey,
# MAGIC   "spark.cosmos.database" -> cosmosDatabaseName,
# MAGIC   "spark.cosmos.container" -> cosmosContainerName
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC fakeFriendsDfLimit_Cast
# MAGIC    .write
# MAGIC    .format("cosmos.oltp")
# MAGIC    .options(cfg)
# MAGIC    .mode("APPEND")
# MAGIC    .save()

# COMMAND ----------

# MAGIC %scala
# MAGIC fakeFriendsDfLimit_Cast.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read from Cosmos

# COMMAND ----------

# MAGIC %scala
# MAGIC // # Primera Forma
# MAGIC val fakeFriends_Df = spark.read.format("cosmos.oltp").options(cfg).load()
# MAGIC fakeFriends_Df.createOrReplaceTempView("vt_fakeFriends_Df")

# COMMAND ----------

# MAGIC %scala
# MAGIC fakeFriends_Df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from vt_fakeFriends_Df

# COMMAND ----------

# Segunda forma
querySelect = "SELECT c.id, c.name, c.age, c.friends FROM c"
list=[]
containerClient = dbClient.get_container_client(newContainerName)
for items in containerClient.query_items( query = querySelect,enable_cross_partition_query = True):
    display(items)
    list.append(items)
display(list)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update

# COMMAND ----------

# MAGIC %md
# MAGIC #### Actualizando solo un item

# COMMAND ----------

# Actualizar un registro
queryUpdated = "SELECT * FROM c"              

updateItem = {'id':'1', 'name': 'Kiara', 'age': 25, 'friends': 5,"date_time":19317}
containerClient.upsert_item(updateItem)

# listar
for items in containerClient.query_items( query = queryUpdated,enable_cross_partition_query = True):
    display(items)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Actualizas algunos items

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Actualizando todo el container, add current_date()

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_update = fakeFriendsDfLimit.withColumn("date_time", current_date())
# MAGIC .withColumn("id", $"id".cast(StringType))

# COMMAND ----------

# MAGIC %scala
# MAGIC df_update.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC df_update.printSchema()

# COMMAND ----------

# MAGIC %scala
# MAGIC df_update.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC df_update
# MAGIC    .write
# MAGIC    .format("cosmos.oltp")
# MAGIC    .options(cfg)
# MAGIC    .mode("append")
# MAGIC    .save()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Delete

# COMMAND ----------

# MAGIC %md
# MAGIC #### Eliminar solo un item

# COMMAND ----------

for item in containerClient.query_items(
        query='SELECT * FROM PersonContainer p WHERE p.name = "Ben"',
        enable_cross_partition_query=True):
    containerClient.delete_item(item, partition_key='11')

# COMMAND ----------

#listar
for items in containerClient.query_items(query=queryRequestSelect,enable_cross_partition_query = True):

        print(items)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Eliminar toda la data del container

# COMMAND ----------

    

# COMMAND ----------

# MAGIC %md
# MAGIC #### Eliminar algunos documentos del container

# COMMAND ----------

for item in containerClient.query_items(
        query='SELECT * FROM PersonContainer p WHERE p.friends = 55',
        enable_cross_partition_query=True):
    containerClient.delete_item( item,partition_key='2')
    
# por ver aun

# COMMAND ----------

# MAGIC %md
# MAGIC #### Eliminar base de datos y container

# COMMAND ----------

# Delete Container
dbClient.delete_container(newContainerName)
print(' Deleted Container ', newContainer)

# COMMAND ----------


# Delete Database
 
client.delete_database(newDatabaseName)
print('\n9. Deleted Database ', newDatabaseName)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RU

# COMMAND ----------


