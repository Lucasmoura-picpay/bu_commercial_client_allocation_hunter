# Databricks notebook source
# MAGIC %md
# MAGIC ## self_service_analytics.bu_commercial_retail_cliente_allocation_hunter
# MAGIC #####jira tasks: <nav>
# MAGIC
# MAGIC
# MAGIC #####last authors: <nav>
# MAGIC   @Lucas Moura
# MAGIC </nav>
# MAGIC
# MAGIC #####last update:
# MAGIC  29/09/2025

# COMMAND ----------

# MAGIC %md
# MAGIC CLIENTES QUE PRECISAM SER ENCARTEIRADOS PELO HUNTER

# COMMAND ----------


spark.sql(""" 
    CREATE OR REPLACE TEMP VIEW bu_commercial_retail_client_allocation_hunter AS
SELECT DISTINCT CONSUMER_IDENTITY_ID AS CONSUMER_ID,
                HUNTER_LABEL AS PORTFOLIO_DESCRIPTION,
                'TRUE' AS IS_INVESTMENT_PORTFOLIO
FROM(SELECT DISTINCT 
                 A.CONSUMER_ID 
                ,B.CONSUMER_IDENTITY_ID
                ,A.OWNER_ID
                ,DATE(A.START_EVENT_AT) AS REFERENCE_DATE
                ,CASE 
                    WHEN A.OWNER_ID = '546' THEN 'Assessor_ppi 22'
                    WHEN A.OWNER_ID = '590' THEN 'Assessor_ppi 23'
                    WHEN A.OWNER_ID = '55' THEN 'Assessor_ppi 24'
                    WHEN A.OWNER_ID = '630' THEN 'Assessor_ppi 25'
                    WHEN A.OWNER_ID = '628' THEN 'Assessor_ppi 27'
                    WHEN A.OWNER_ID = '629' THEN 'Assessor_ppi 28'
                    WHEN A.OWNER_ID = '315' THEN 'Assessor_ppi 29'
                ELSE A.OWNER_ID
                END AS HUNTER_LABEL

FROM self_service_analytics.bu_commercial_retail_lead_hunter_total A
INNER JOIN consumers.sec_consumers_identifies B ON A.CONSUMER_ID = B.CONSUMER_ID
LEFT ANTI JOIN (SELECT DISTINCT A.CONSUMER_ID 
                FROM commercial_retail.sales_performance_consumers A 
                WHERE A.INVEST_MANAGER_ID IN (55,546,590, 628, 630, 629, 315) ) C ON A.CONSUMER_ID = C.CONSUMER_ID

WHERE DATE(A.START_EVENT_AT) = CURRENT_DATE()
AND A.OWNER_ID IN ('55','630','546','590', '628', '629', '315')) 
""")

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

_output_df_ = spark.sql('''
         select 
         (monotonically_increasing_id() + 1) as BU_COMMERCIAL_RETAIL_CLIENT_ALLOCATION_HUNTER_ID,
         *
         from
         bu_commercial_retail_client_allocation_hunter
 ''')