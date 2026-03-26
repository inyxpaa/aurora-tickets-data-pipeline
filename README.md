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

## Gastos

El proyecto utiliza un modelo de pago por uso. Esta es la estimación mensual si la infraestructura estuviera encendida 24 horas al día:

* **Amazon EC2 (6 instancias t3.medium):** 50$ aprox.
* **Amazon RDS (MySQL db.t3.medium):** 15$ aprox.
* **Amazon S3 y CloudWatch:** Menos de 1$.
* **Coste total estimado:** **66$ al mes**.
* **Coste real del proyecto:** Al usar el entorno de AWS Academy, los recursos se apagan tras la ejecución. El coste real de esta práctica ha sido de apenas unos **4$**.

## Conclusiones

El proyecto Aurora Tickets ha sido un éxito. Hemos construido un pipeline de datos completo en la nube de AWS. La infraestructura es escalable. El procesamiento con Spark es rápido y eficiente. El sistema permite la toma de decisiones basada en datos reales a través de los paneles de visualización.
