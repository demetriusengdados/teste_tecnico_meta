'''Questão 2: Média de faturamento por conta
Descrição:
Calcular a média de faturamento de cada conta nos últimos 3 e 6 meses retroativos a janeiro de 2020. Quando não houver dados suficientes, o valor será null.

Solução:
Ler o arquivo invoices.csv com PySpark.
Filtrar os dados por intervalo de datas.
Agregar os dados para calcular as médias.
Lidar com casos de ausência de dados.'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, expr
from pyspark.sql.window import Window

# Configurar sessão Spark
spark = SparkSession.builder.appName("InvoiceAverage").getOrCreate()

# Carregar dados
file_path = "/mnt/data/invoices.csv"
invoices_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Conversão de data para formato correto
invoices_df = invoices_df.withColumn("date", expr("to_date(date, 'yyyy-MM-dd')"))

# Filtrar dados relevantes
filtered_df = invoices_df.filter(col("date") <= "2020-01-31")

# Criar janelas de tempo
window_3_months = Window.partitionBy("customer", "account").orderBy("date").rangeBetween(-90, 0)
window_6_months = Window.partitionBy("customer", "account").orderBy("date").rangeBetween(-180, 0)

# Calcular médias
result_df = filtered_df.withColumn("avg_invoices_last_3_months", avg("amount").over(window_3_months)) \
                       .withColumn("avg_invoices_last_6_months", avg("amount").over(window_6_months))

# Exibir resultados
result_df.show()
