from yasmss.parsetemplate import parser
from yasmss.driver import driver


query = 'SELECT * FROM USERS INNERJOIN ZIPCODES ON USERS.ZIPCODE == ZICODES.ZIPCODE WHERE ZIPCODES.CITY == PILANI'
parsedq = parser.Parse()
parsedQuery = parsedq.parseQuery(query)
driveq = driver.RunQuery(parsedQuery)
