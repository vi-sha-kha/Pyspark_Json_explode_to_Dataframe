# Databricks notebook source
import requests
import json

# COMMAND ----------

sample_data1= '''{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
} 
'''

# COMMAND ----------

sample_data2='''{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"image":
		{
			"url": "images/0001.jpg",
			"width": 200,
			"height": 200
		},
	"thumbnail":
		{
			"url": "images/thumbnails/0001.jpg",
			"width": 32,
			"height": 32
		}
}'''

# COMMAND ----------

dbutils.fs.put('tmp/sample1.json',sample_data1, overwrite=True)
dbutils.fs.put('tmp/sample2.json',sample_data2, overwrite=True)

# COMMAND ----------

path1=dbutils.fs.ls('tmp/sample1.json')[0][0]
path2=dbutils.fs.ls('tmp/sample2.json')[0][0]

# COMMAND ----------

path1, path2

# COMMAND ----------

sample_data1_df=spark.read.json(path1, multiLine=True)

# COMMAND ----------

sample_data2_df=spark.read.json(path2, multiLine=True)

# COMMAND ----------

sample_data1_df.display()

# COMMAND ----------

sample_data2_df.display()

# COMMAND ----------

sample_data2_df.select('image.*').display()

# COMMAND ----------

sample_data2_df.select('image.*','*','thumbnail.*').display()

# COMMAND ----------

sample_data2_df.select('image.*','*','thumbnail.*').drop('image','thumbnail').display()

# COMMAND ----------

sample_data1_df.display()

# COMMAND ----------

sample_data1_df.select('batters.*','*').drop('batters').display()

# COMMAND ----------

from pyspark.sql.functions import explode,col
sample_data1_df.select('batters.*','*').drop('batters').select(explode(col('batter')).alias('batter_exploded'),'*').alias('batter_exploded').drop('batter').select('batter_exploded.*').drop('batter_exploded').display()

# COMMAND ----------

from pyspark.sql.functions import explode,col
sample_data1_df.select(explode(col('topping')).alias('topping_exploded'),'*').drop('topping').select('topping_exploded.*').display()

# COMMAND ----------

sample3='''{
	"items":
		{
			"item":
				[
					{
						"id": "0001",
						"type": "donut",
						"name": "Cake",
						"ppu": 0.55,
						"batters":
							{
								"batter":
									[
										{ "id": "1001", "type": "Regular" },
										{ "id": "1002", "type": "Chocolate" },
										{ "id": "1003", "type": "Blueberry" },
										{ "id": "1004", "type": "Devil's Food" }
									]
							},
						"topping":
							[
								{ "id": "5001", "type": "None" },
								{ "id": "5002", "type": "Glazed" },
								{ "id": "5005", "type": "Sugar" },
								{ "id": "5007", "type": "Powdered Sugar" },
								{ "id": "5006", "type": "Chocolate with Sprinkles" },
								{ "id": "5003", "type": "Chocolate" },
								{ "id": "5004", "type": "Maple" }
							]
					},
                    {
						"id": "0002",
						"type": "donut2",
						"name": "Cake2",
						"ppu": 0.55,
						"batters":
							{
								"batter":
									[
										{ "id": "1001", "type": "Regular" },
										{ "id": "1002", "type": "Chocolate" },
										{ "id": "1003", "type": "Blueberry" },
										{ "id": "1004", "type": "Devil's Food" }
									]
							},
						"topping":
							[
								{ "id": "5001", "type": "None" },
								{ "id": "5002", "type": "Glazed" },
								{ "id": "5005", "type": "Sugar" },
								{ "id": "5007", "type": "Powdered Sugar" },
								{ "id": "5006", "type": "Chocolate with Sprinkles" },
								{ "id": "5003", "type": "Chocolate" },
								{ "id": "5004", "type": "Maple" }
							]
					}
				]
		}
}'''

# COMMAND ----------

dbutils.fs.put('/tmp/sample3.json',sample3,overwrite=True)

# COMMAND ----------

path3=dbutils.fs.ls('tmp/sample3.json')[0][0]

# COMMAND ----------

path3

# COMMAND ----------

sample_data3_df=spark.read.json(path3, multiLine=True)

# COMMAND ----------

sample_data3_df.display()

# COMMAND ----------

sample_data3_df.select('items.*','*').drop('items').display()

# COMMAND ----------

from pyspark.sql.functions import explode,col
sample_data3_df.select('items.*','*').drop('items').select(explode(col('item')).alias('item_exploded')).display()

# COMMAND ----------

from pyspark.sql.functions import explode,col
sample_data3_df.select('items.*','*').drop('items').select(explode(col('item')).alias('item_exploded')).select('item_exploded.*').drop('item_exploded').select('batters.*','*').drop('batters').select(explode('batter').alias('batter_exploded'),'*').drop('batter').select(explode(col('topping')).alias('topping_exploded'),'*').select('topping_exploded.*','*').drop('batter_exploded').drop('topping').drop('topping_exploded').display()

# COMMAND ----------


