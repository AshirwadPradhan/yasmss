"""Get the parsed query from the driver and apply transformation and action based on the
    query template
"""
from pyspark.sql import SparkSession
from yasmss.schema import schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

baseURI = 'hdfs://localhost:9000/user/dominoUzu/input/'
table_format = '.csv'


class SparkJob:
    """Start a spark job based on the query template provided by the user
    """

    def __init__(self):
        self.queryresult = None
        self.trans_actions = None
        self.exectime = None

    def _prepareEnv(self):
        spark = SparkSession.builder.master(
            'local').appName('example').getOrCreate()
        return spark

    def _getKeyType(self, keyType):
        if keyType == 'IntegerType':
            return IntegerType()
        elif keyType == 'StringType':
            return StringType()
        else:
            raise TypeError(keyType+' is not supported')

    def _getdata(self, table):
        """Read from csv using sprak.read.csv with schema
            Make a YAML file to specify schema and get StructType 
        """
        spark = self._prepareEnv()
        table_schema_dict = schema.Schema().getSchemaDict(table=table)
        table_schema_structlist = []

        for key, value in table_schema_dict.items():
            table_schema_structlist.append(
                StructField(key, self._getKeyType(value), True))
        table_schema = StructType(table_schema_structlist)

        table_data = spark.read.csv(
            baseURI+table+table_format, schema=table_schema)
        return table_data

    def startjob(self, queryset, classType):
        if classType == 'QuerySetJoin':
            df_fromtabledata = self._getdata(queryset.fromtable)
            df_jointabledata = self._getdata(queryset.jointable)
            print(df_fromtabledata.show())
            print(df_jointabledata.show())
            on_l = queryset.onlval.split('.')
            on_r = queryset.onrval.split('.')
            if on_l[1] != on_r[1]:
                print('Error in attribute')
                return
            joined_rdd = df_fromtabledata.join(
                df_jointabledata, on=on_l[1], how='inner').orderby(on_l[1], ascending='True')
        elif classType == 'QuerySetGroupBy':
            pass
        else:
            print('Unknown class Error')
