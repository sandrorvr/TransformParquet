import os
import subprocess

class KpisTolls:

  @staticmethod
  def listDirHDFS(path):
    cmd = f'hdfs dfs -ls {path}'
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')
  
  @staticmethod
  def listChunks(phase, folder):
    path = f'/project/dz/collab/PD_SA_Analytics/data_accuracy/dev_files/{phase}/transform_zone/{folder}'
    cmd = f'hdfs dfs -ls {path}'
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')
  
  def countListOfChunks(phase, folder):
    path = f'/project/dz/collab/PD_SA_Analytics/data_accuracy/dev_files/{phase}/transform_zone/{folder}'
    chunksFolder = KpisTolls.listChunks(phase, folder)
    for ck in chunksFolder:
      count = spark.read.parquet(f'{path}/{ck}').count()
      print(f'CHUNK: {ck} | COUNT: {count}')