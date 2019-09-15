from parsetemplate import parser
from driver import driver


query = 'SELECT * FROM USERS INNERJOIN ZIPCODES ON USERS.ZIPCODE = ZICODES.ZIPCODE WHERE ZIPCODES.CITY = NYACK'
parsedq = parser.Parse()
parsedQuery = parsedq.parseQuery(query)
driveq = driver.RunQuery(parsedQuery)
driveq.run()
