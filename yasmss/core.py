from parsetemplate import parser
from driver import driver
from flask import Flask, request
from flask_restful import Resource, Api, reqparse

app = Flask(__name__)
api = Api(app)

query = ""
ps = reqparse.RequestParser()
ps.add_argument('query')


class Home(Resource):
    def get(self):
        # Send the index Page as a response
        return "Hello World!"


class RunQuery(Resource):
    def get(self):
        pass

    def post(self):
        args = ps.parse_args()
        query = args['query']
        parsedq = parser.Parse()
        parsedQuery = parsedq.parseQuery(query.upper())
        driveq = driver.RunQuery(parsedQuery)
        runquery_op = driveq.run()
        # json_runq = jsonizer.covrtJSon(runquery_op)
        resp_d = {'result': runquery_op}
        return resp_d, 201


api.add_resource(Home, '/', '/index', '/home')
api.add_resource(RunQuery, '/query')

if __name__ == "__main__":
    app.run(debug=True)

# query = 'SELECT * FROM USERS INNERJOIN ZIPCODES ON USERS.ZIPCODE = ZICODES.ZIPCODE WHERE ZIPCODES.CITY = NEW YORK'
# query = 'SELECT MOVIEID,SUM(RATING) FROM RATING GROUPBY MOVIEID HAVING SUM(RATING) > 1000'
# parsedq = parser.Parse()
# parsedQuery = parsedq.parseQuery(query.upper())
# print(parsedQuery)
# driveq = driver.RunQuery(parsedQuery)
# runquery_op = driveq.run()
# print("COMPLETED: "+runquery_op.sparkOutput)
