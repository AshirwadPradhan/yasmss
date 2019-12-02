"""Parses the schemas.yaml file and get the schema of the tables in the HDFS
"""
import yaml


class Schema:
    def __init__(self):
        if __name__ != "__main__":
            with open("schema/schemas.yaml", 'r') as file:
                data = yaml.load(file, Loader=yaml.FullLoader)
                self.schema = data
        elif __name__ == "__main__":
            with open("schemas.yaml", 'r') as file:
                data = yaml.load(file, Loader=yaml.FullLoader)
                self.schema = data

    def getSchemaDict(self, table):
        table_schema = self.schema[table]
        return table_schema
