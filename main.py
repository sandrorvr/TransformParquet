from pyspark.sql import SparkSession

from .kpis import Kpi1
from .transformations import FormatDateFilter
from .lib.Load import Load
from .lib.Log import Log

#****************CONFIG SAPARK**************************

spark = SparkSession.builder.master("local[*]").getOrCreate()

#********************Get DATAFRAME*********************
data = [
        (1,1,'v1','2000-05-01', 'begin',0),
        (2,2,'v1','2000-05-02', 'run',1),
        (3,3,'v1','2000-05-03', None,0),
        (4,4,'v1','2000-05-04', None,0),
        (5,5,'v1','2000-05-05', 'off',0),
        (6,6,'v1','2000-05-06', 'end',0),
        (7,7,'v1','2000-05-07', 'run',0),
        (8,7,'v1','2000-05-07', 'run',0),
        (9,8,'v1','2000-05-08', 'rum',1),
        (10,9,'v1','2000-05-09', 'begin',1),
        (11,10,'v1','2000-05-10', 'end',0),
        (12,10,'v1','2000-05-11', 'begin',0),
        (13,10,'v1','2000-05-12', None,1),
        (14,13,'v1','2000-05-13', None,0),
        (15,14,'v1','2000-05-14', None,0),
        (16,15,'v1','2000-05-15', 'run',1),
        (17,16,'v1','2000-05-16', 'run',1),
        (18,17,'v1','2000-05-17', 'run',0),
        (19,18,'v1','2000-05-01', 'begin',0),
        (20,19,'v1','2000-05-02', 'run',1),
        (21,20,'v1','2000-05-03', None,0),
        (22,21,'v1','2000-05-04', None,0),
        (23,23,'v1','2000-05-05', 'off',0),
        (24,23,'v1','2000-05-06', 'end',0),
        (25,24,'v2','2000-05-07', 'begin',0),
        (26,25,'v2','2000-05-08', 'run',1),
        (27,26,'v2','2000-05-09', 'run',1),
        (28,27,'v2','2000-05-10', 'end',0),
        (29,28,'v2','2000-05-11', 'begin',0),
        (30,28,'v2','2000-05-12', None,1),
        (31,30,'v2','2000-05-13', None,0),
        (32,31,'v2','2000-05-14', None,0),
        (33,32,'v2','2000-05-15', 'run',1),
        (34,33,'v2','2000-05-16', 'run',1),
        (35,34,'v2','2000-05-17', 'end',0)
      ]

columns = ['INDEX_LOOP','id','vin','time','motiveMode','check_VHA']
df = spark.createDataFrame(data=data, schema = columns)

df.show(truncate=False, n=50)


#********SET KPIS******

KPI1 = Kpi1(titleKPI='kpi1', columnsUsed=['id'])\
        .setLabels('PAR')\
        .setLabels('IMPAR')

KPI2 = Kpi1(titleKPI='kpi2', columnsUsed=['id'])\
        .setLabels('IMPAR')\
        .setLabels('PAR')


#************SET LOAD**********
Load(\
     df,
     path='./',
     phase='phase',
     folder='folder', 
     n_batch=10
     )\
     .setKpis(KPI1)\
     .setKpis(KPI2)\
     .setLog(Log('phase'))\
     .setTransformations(formatDateFilter('time','yyyy-MM'))\
     .loop()