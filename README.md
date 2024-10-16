# Procesamiento de Datos con PySpark


Descripción de la Solución
Este proyecto utiliza PySpark para realizar el procesamiento de un conjunto de datos almacenado en un archivo CSV. La solución incluye las siguientes etapas:

Inicialización de la Sesión de Spark: Se establece una sesión de Spark para la ejecución de las tareas.
Carga de Datos: Se cargan los datos desde un archivo CSV ubicado en el sistema de archivos HDFS.
Limpieza de Datos:
Se eliminan registros duplicados.
Se eliminan filas que contienen valores nulos.
Análisis Exploratorio de Datos (EDA): Se generan estadísticas básicas del conjunto de datos limpio.
Guardado de Datos Procesados: Los datos limpiados se guardan en un nuevo archivo CSV en el sistema de archivos local.
