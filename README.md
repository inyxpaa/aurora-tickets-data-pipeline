# Aurora Tickets - Data Engineering Pipeline

Proyecto de procesamiento de datos masivos y analítica para una plataforma de venta de entradas. 

## Arquitectura del Proyecto

El sistema procesa logs de servidores web y bases de datos relacionales mediante una arquitectura en la nube:

* **Ingesta:** Generación de eventos y almacenamiento en Amazon S3.
* **Procesamiento:** Clúster de Apache Spark en modo Standalone (EC2). Limpieza y cruce de datos.
* **Almacenamiento:** Data Lake en Amazon S3 (formato Parquet) y Data Warehouse en Amazon RDS (MySQL).
* **Analítica:** Monitorización en tiempo real y Dashboards de negocio con Amazon CloudWatch Logs Insights.

## Tecnologías Utilizadas

* **Cloud:** Amazon Web Services (EC2, S3, RDS, CloudWatch, IAM).
* **Big Data:** Apache Spark, PySpark.
* **Bases de Datos:** MySQL, Parquet.
* **Lenguajes:** Python, SQL.

## Estructura del Repositorio

* `/src`: Scripts de procesamiento en PySpark (Curación y Analítica).
* `/sql`: Esquemas de base de datos y consultas de CloudWatch.
* `/docs`: Memoria técnica detallada del proyecto y diagramas de arquitectura.
