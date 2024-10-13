# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea33').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea33/rows.csv'

# Cargar el conjunto de datos desde la fuente original
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprimir el esquema para verificar los tipos de datos
df.printSchema()

# Mostrar las primeras filas del DataFrame
df.show(5)

# Eliminar duplicados
df_cleaned = df.dropDuplicates()

# Eliminar filas con valores nulos
df_cleaned = df_cleaned.na.drop()

# Mostrar algunos registros después de la limpieza
print("Datos después de la limpieza\n")
df_cleaned.show(5)


# Realizar análisis exploratorio de datos (EDA)
# Estadísticas básicas
print("Resumen estadístico básico\n")
df_cleaned.summary().show()


# Guardar los datos procesados en un archivo CSV en el Escritorio y sobrescribir si el archivo ya existe
df_cleaned.write.mode('overwrite').format('csv').option('header', 'true').save('file:///home/vboxuser/Escritorio/cleaned_data.csv')