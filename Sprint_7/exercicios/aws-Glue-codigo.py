import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['JOB_NAME','S3_INPUT_PATH','S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

df = glueContext.create_dynamic_frame.from_options(
"s3",
{
"paths": [
source_file
]
},
"csv",
{"withHeader": True, "separator":","},
)

only_1934 = df.filter(lambda row: row['anoLancamento']=='1934')

only_1934.printSchema()

var = df.apply_mapping([
    ("nome", "string", "nome", "string")
], transformation_ctx="apply mapping")
var = df.select_fields(['ano', 'sexo', 'nome', 'total'])

contagem_de_linhas = var.count()
print(f"Contagem de Linhas: {contagem_de_linhas}")

nome_feminino_mais_registros = var.toDF().filter(col("sexo") == "F").groupBy("nome").count().orderBy(desc("count")).first()
nome_feminino = nome_feminino_mais_registros['nome'] 
contagem_feminina = nome_feminino_mais_registros['count']
print(f"Nome feminino com mais registros: (nome feminino) (Contagem:{contagem_feminina})")

nome_masculino_mais_registros = var.toDF().filter(col("sexo") == "M").groupBy("nome").count().orderBy(desc("count")).first()
nome_masculino = nome_masculino_mais_registros['nome']
contagem_masculina = nome_masculino_mais_registros['count']
print(f"Nome masculino com mais registros: (nome masculino) (Contagem:{contagem_masculina})")

contagem_de_nomes = var.toDF().groupBy(["ano", "sexo"]).count().orderBy(desc("count"))
print("Contagem de Nomes, agrupados por ano e sexo:")
contagem_de_nomes.show()

Limited_var = var.toDF().limit(10)
Limited_var.createOrReplaceTempView("nomes_limitados")
total_de_registros_no_ano = spark.sql("""
    SELECT ano, COUNT(*) AS count
    FROM nomes_limitados
    GROUP BY ano
    ORDER BY ano
""")
print("Total de registros por ano:")
total_de_registros_no_ano.show()

glueContext.write_dynamic_frame.from_options(
frame = var,
connection_type = "s3",
connection_options = {"path": target_path, "partitionKeys":["sexo","ano"]},
format = "json")

job.commit()
