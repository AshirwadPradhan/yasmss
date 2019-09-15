"""Get the parsed query from the driver and apply transformation and action based on the
    query template
"""
from pyspark.sql import SparkSession
from schema import schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as f
import time

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

    def _computeaggr(self, df_fromtable, queryset):
        df_tempg = df_fromtable.groupBy(queryset.groupcolumns)
        df_tempga = df_tempg.agg({str(queryset.aggcol): str(queryset.aggfunc)})
        return df_tempga

    def startjob(self, queryset, classType):
        if classType == 'QuerySetJoin':
            start_time = time.time()
            df_fromtabledata = self._getdata(queryset.fromtable)
            df_jointabledata = self._getdata(queryset.jointable)

            on_l = queryset.onlval.split('.')
            on_r = queryset.onrval.split('.')
            if on_l[1] != on_r[1]:
                raise AttributeError(
                    'Lval and Rval of "On" condition does not match')

            df_innerjoin = df_fromtabledata.join(
                df_jointabledata, on=on_l[1], how='inner').orderBy(on_l[1], ascending=True)

            wherecol = queryset.wherelval.split('.')[1]
            try:
                con_whererval = int(queryset.whererval)
                filter_cond = wherecol+queryset.whereop+queryset.whererval
            except ValueError:
                filter_cond = wherecol+queryset.whereop+'"'+queryset.whererval+'"'
            df_finalres = df_innerjoin.where(filter_cond)
            self.queryresult = df_finalres
            self.trans_actions = ['join', 'where']
            self.exectime = (time.time() - start_time)

        elif classType == 'QuerySetGroupBy':
            start_time = time.time()
            df_fromtable = self._getdata(queryset.fromtable)
            df_agg_groupby = self._computeaggr(df_fromtable, queryset)
            df_finalres = df_agg_groupby.where(queryset.havcond)
            self.queryresult = df_finalres
            self.trans_actions = ['groupby', 'agg', 'where']
            self.exectime = (time.time() - start_time)

        else:
            raise TypeError('Unidentified Class Type')
