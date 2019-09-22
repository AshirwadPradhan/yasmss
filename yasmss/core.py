from parsetemplate import parser
from driver import driver
from flask import Flask, request
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)

query = ""
ps = reqparse.RequestParser()
ps.add_argument('query')


class RunQuery(Resource):
    def get(self):
        pass

    def post(self):
        args = ps.parse_args()
        query = args['query']
        try:
            parsedq = parser.Parse()
            parsedQuery = parsedq.parseQuery(query.upper())
        except NotImplementedError as e:
            print(e)
            return {"Err": e}, 400
        except NameError as e:
            print(e)
            return {"Err:": e}, 400
        except ValueError as e:
            print(e)
            return {"Err:": e}, 400
        except Exception as e:
            print(e)
            return {"Err:": "Error"}, 400

        try:
            driveq = driver.RunQuery(parsedQuery)
            runquery_op = driveq.run()
        except TypeError as e:
            print(e)
            return {"Err:": e}, 400
        except Exception as e:
            print(e)
            return {"Err:": "Error"}, 400
        resp_d = {'MRresult': runquery_op.mrOutput,
                  'SparkResult': runquery_op.sparkOutput}
        return resp_d, 201


api.add_resource(RunQuery, '/query')

if __name__ == "__main__":
    app.run(debug=True)

# query = 'SELECT * FROM USERS INNERJOIN ZIPCODES ON USERS.ZIPCODE = ZICODES.ZIPCODE WHERE ZIPCODES.CITY = NEW YORK'
# query = 'SELECT MOVIEID,SUM(RATING) FROM RATING GROUPBY MOVIEID HAVING SUM(RATING) > 1000'
