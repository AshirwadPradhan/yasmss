from parsetemplate import parser
from driver import driver


query = 'SELECT * FROM USERS INNERJOIN ZIPCODES ON USERS.ZIPCODE = ZICODES.ZIPCODE WHERE ZIPCODES.CITY = NYACK'
# query = 'SELECT MOVIEID,SUM(RATING) FROM RATING GROUPBY MOVIEID HAVING SUM(RATING) > 1000'
parsedq = parser.Parse()
parsedQuery = parsedq.parseQuery(query.upper())
# print(parsedQuery)
driveq = driver.RunQuery(parsedQuery)
runquery_op = driveq.run()
print("COMPLETED: "+runquery_op.sparkOutput)
