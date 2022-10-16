from lib.PRE_SAVE import PRE_SAVE

class FormatDateFilter(PRE_SAVE):
  def __init__(self, columnDate, formatDate):
    super().__init__()
    self.columnDate = columnDate
    self.formatDate = formatDate
  
  def configTransformation(self):
    self._df = self._df.withColumn(\
                                   self.columnDate,
                                   f.date_format(self.columnDate, self.formatDate)\
                                   .alias(self.columnDate)
                                   )
  def __str__(self):
    return 'formatDateFilter'