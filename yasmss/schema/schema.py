import yaml


class Schema:
    def __init__(self):
        with open("schemas.yaml", 'r') as file:
            data = yaml.load(file, Loader=yaml.FullLoader)
        self.schema = data

    def getSchemaDict(self, table):
        table_schema = self.schema[table]
        return table_schema


if __name__ == '__main__':
    print(Schema().getSchemaDict(table='users'))
