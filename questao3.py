'''Questão 3: Relatório Consolidado de Respostas
Descrição:
Consolidar as respostas informadas pelos usuários nas horas 13 e 14 em um formato tabular.

Solução:
Ler arquivos JSON hour=13.json e hour=14.json.
Explodir os campos content para capturar respostas.
Agregar para identificar o first_answer_dt e last_answer_dt.
Retornar os dados em um formato dinâmico.'''

from pyspark.sql.functions import col, explode, lit, min, max, when
import pyspark.sql.functions as F

# Carregar arquivos JSON
hour_13_path = "/mnt/data/hour=13.json"
hour_14_path = "/mnt/data/hour=14.json"

df_13 = spark.read.json(hour_13_path)
df_14 = spark.read.json(hour_14_path)

# Combinar dados
combined_df = df_13.union(df_14)

# Explodir conteúdo
exploded_df = combined_df.select(
    "customer", "flow", "session", "timestamp", 
    F.explode("content").alias("key", "value")
)

# Filtrar respostas válidas
filtered_df = exploded_df.filter(col("value") != "")

# Obter primeiras e últimas interações
aggregated_df = filtered_df.groupBy("customer", "flow", "session").agg(
    min("timestamp").alias("first_answer_dt"),
    max("timestamp").alias("last_answer_dt"),
    F.map_from_entries(F.collect_list(F.struct("key", "value"))).alias("answers")
)

# Exibir resultados
aggregated_df.show(truncate=False)
