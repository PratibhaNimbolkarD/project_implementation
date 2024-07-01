# Databricks notebook source
df=spark.read.format('json').option('multiline',True).load("dbfs:/FileStore/my_data/sample_json_3.json")
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DateType, BooleanType

# COMMAND ----------




child_trainingData_items_prop_child_issues= StructType([
     StructField("properties",StructType([
         StructField("countryDataIssue",StructType([StructField("type",StringType(),True)]),True),
         StructField("dateFormatIssue", StructType([StructField("type",StringType(), True)]),True)
                     ]),True),
     StructField("type",StringType(),True)
 ])


child_trainingData_items_prop_child_date= StructType([
     StructField("format",StringType(),True),
     StructField("type", StringType(), True)
 ])

child_trainingData_items_prop_child_data=StructType([
    StructField("properties", StructType([
        StructField("field1", StructType([
            StructField("type", StringType(), True)
        ])),
        StructField("field2", StructType([
            StructField("type", StringType(), True)
        ])),
        StructField("field3", StructType([
            StructField("type", StringType(), True)
        ]))
    ]), True),
    StructField('required',ArrayType(StringType(),True),True),
    StructField("type", StringType(), True)
])

child_trainingData_items_prop_country=StructType([
    StructField("enum", ArrayType(StructType([
        StructField("element", StringType(), True)
        ])
    ),True),
    StructField('type',StringType(),True)
])



child_trainingData_items_prop=StructType([
    StructField('country',child_trainingData_items_prop_country,True),
    StructField('data',child_trainingData_items_prop_child_data,True),
    StructField('date',child_trainingData_items_prop_child_date,True),
    StructField('issues',child_trainingData_items_prop_child_issues,True),
])

child_trainingData_items=StructType([
    StructField('properties',child_trainingData_items_prop,True),
    StructField('required',ArrayType(StringType(),True),True),
    StructField('type',StringType(),True)
])


child_trainingData=StructType([
    StructField('item',child_trainingData_items,True),
    StructField('type',StringType(),True),
])

child_meta_data_prop=StructType([
    StructField("createdAt",StructType([   ###
        StructField("format",StringType(), True),
        StructField("type",StringType(),True)
    ]),True),
    StructField("createdBy",StructType(StructType([
        StructField("type",StringType(),True)
        ])
    ), True),
    StructField("version", StructType(StructType([
        StructField("type", StringType(), True)
        ])
    ), True)
])


child_meta_data=StructType([
    StructField('properties',child_meta_data_prop,True),
    StructField('type',StringType(),True)
])


child_prop=StructType([
    StructField('metadata',child_meta_data,True ),
    StructField('traaingData',child_trainingData,True ),
])

custom_root_schema=StructType([
    StructField('type',StringType(),True),
    StructField('properties',child_prop,True),
    StructField('required',ArrayType(StringType(),True),True)
])








# COMMAND ----------

df1= spark.read.json('dbfs:/FileStore/my_data/sample_json_3.json', multiLine= True, schema=custom_root_schema)
df1.printSchema()
