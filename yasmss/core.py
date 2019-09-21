from parsetemplate import parser
from driver import driver
from flask import Flask, request, render_template
from flask_restful import Resource, Api, reqparse
from forms.forms import QueryForm


app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = 'you-will-never-guess'

query = ""
ps = reqparse.RequestParser()
ps.add_argument('query')

class Home(Resource):
    @app.route('/', methods=['GET', 'POST'])
    @app.route('/index',  methods=['GET', 'POST'])
    def index():
        form = QueryForm()
        if form.validate_on_submit():
            print(form.query.data)
            query = form.query.data
            parsedq = parser.Parse()
            parsedQuery = parsedq.parseQuery(query.upper())
            driveq = driver.RunQuery(parsedQuery)
            runquery_op = driveq.run()
            resp_d = {'MRresult': runquery_op.mrOutput, 'SparkResult': runquery_op.sparkOutput}
            return resp_d, 201
        return render_template('index.html', form=form)

    def get(self):
        # Send the index Page as a response
        return "Hello World!"

    @app.route('/test')
    def test():
        return render_template('test.html')

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
        resp_d = {'MRresult': runquery_op.mrOutput, 'SparkResult': runquery_op.sparkOutput}
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
