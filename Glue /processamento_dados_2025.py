import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Inicialização padrão do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Variáveis de Configuração
# Caminho de origem (onde estão os CSVs)
source_path = "s3://tech-challenge-018298043465/bases_origem_pesquisas/2025/"

# Caminho de destino (onde os dados serão salvos em Parquet)
target_path = "s3://tech-challenge-018298043465/bases_finais_pesquisa/2025/"

# Nome do banco de dados no Athena
db_name = "tech_challenge_db" 

# Nome da tabela que será criada no Athena
table_name = "pesquisas_2025"

# 2. Leitura dos dados CSV
dynamic_frame_read = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [source_path], "recurse": True},
    transformation_ctx="dynamic_frame_read"
)

# 3. Escrita em Parquet e Criação da Tabela no Athena
sink = glueContext.getSink(
    path=target_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="sink"
)

# Configura o destino para o catálogo de dados do Glue (que o Athena lê)
sink.setCatalogInfo(catalogDatabase=db_name, catalogTableName=table_name)
sink.setFormat("glueparquet")
sink.writeFrame(dynamic_frame_read)

job.commit()
