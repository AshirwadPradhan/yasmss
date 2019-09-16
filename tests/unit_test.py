import pytest
from yasmss.parsetemplate.parser import Parse


def test_join_normal():
    query = 'SELECT * FROM ZIPCODE INNERJOIN USER ON ZIPCODE.ZIP = USER.ZIP WHERE ZIPCODE.CITY = PILANI'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'zipcode'
    assert q_res.onop == '='
    assert q_res.onlval == 'zipcode.zip'
    assert q_res.onrval == 'user.zip'
    assert q_res.whereop == '='
    assert q_res.wherelval == 'zipcode.city'
    assert q_res.whererval == 'PILANI'


def test_join_case1():
    query = 'SELECT * FROM USER INNERJOIN ZIPCODE ON USER.ZIP = ZIPCODE.ZIP WHERE USER.CITY = PILANI'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'user'
    assert q_res.onop == '='
    assert q_res.onlval == 'user.zip'
    assert q_res.onrval == 'zipcode.zip'
    assert q_res.whereop == '='
    assert q_res.wherelval == 'user.city'
    assert q_res.whererval == 'PILANI'


def test_join_case2():
    query = 'SELECT * FROM USER INNERJOIN ZIPCODE ON ZIPCODE.ZIP = USER.ZIP WHERE USER.CITY = PILANI'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'user'
    assert q_res.onop == '='
    assert q_res.onlval == 'zipcode.zip'
    assert q_res.onrval == 'user.zip'
    assert q_res.whereop == '='
    assert q_res.wherelval == 'user.city'
    assert q_res.whererval == 'PILANI'


def test_join_case3():
    query = 'SELECT * FROM USER INNERJOIN ZIPCODE ON ZIPCODE.ZIP = USER.ZIP WHERE ZIPCODE.CITY < PILANI'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'user'
    assert q_res.onop == '='
    assert q_res.onlval == 'zipcode.zip'
    assert q_res.onrval == 'user.zip'
    assert q_res.whereop == '<'
    assert q_res.wherelval == 'zipcode.city'
    assert q_res.whererval == 'PILANI'


def test_join_case3_F():
    query = 'SELECT * FROM USER INNERJOIN ZIPCODE ON ZIPCODE.ZIP = USER.ZIP WHERE ZIPCODE.CITY == PILANI'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'user'
    assert q_res.onop == '='
    assert q_res.onlval == 'zipcode.zip'
    assert q_res.onrval == 'user.zip'
    assert q_res.whereop == '='
    assert q_res.wherelval != 'user.city'
    assert q_res.whererval == 'PILANI'


def test_join_case4():
    query = 'SELECT * FROM MOVIES INNERJOIN RATING ON MOVIEs.MOVIEID = RATING.MOVIEID WHERE MOVIE.ACTION = 1'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'movies'
    assert q_res.onop == '='
    assert q_res.onlval == 'movies.movieid'
    assert q_res.onrval == 'rating.movieid'
    assert q_res.whereop == '='
    assert q_res.wherelval == 'movie.action'
    assert q_res.whererval == '1'


def test_join_case5():
    query = 'SELECT * FROM MOVIES INNERJOIN RATING ON MOVIEs.MOVIEID = RATING.MOVIEID WHERE MOVIE.ACTION > 1'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'movies'
    assert q_res.onop == '='
    assert q_res.onlval == 'movies.movieid'
    assert q_res.onrval == 'rating.movieid'
    assert q_res.whereop == '>'
    assert q_res.wherelval == 'movie.action'
    assert q_res.whererval == '1'


def test_join_space1():
    query = ' SELECT   *  FROM   MOVIES   INNERJOIN  RATING  ON    MOVIEs.MOVIEID =  RATING.MOVIEID WHERE MOVIE.ACTION  > 1'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'movies'
    assert q_res.onop == '='
    assert q_res.onlval == 'movies.movieid'
    assert q_res.onrval == 'rating.movieid'
    assert q_res.whereop == '>'
    assert q_res.wherelval == 'movie.action'
    assert q_res.whererval == '1'


def test_join_space2():
    query = 'SELECT * FROM MOVIES INNERJOIN RATING ON MOVIEs. MOVIEID =  RATING  .  MOVIEID WHERE MOVIE .  ACTION  > 1'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'movies'
    assert q_res.onop == '='
    assert q_res.onlval == 'movies.movieid'
    assert q_res.onrval == 'rating.movieid'
    assert q_res.whereop == '>'
    assert q_res.wherelval == 'movie.action'
    assert q_res.whererval == '1'


def test_join_wherespace():
    query = 'SELECT * FROM USERS INNERJOIN ZIPCODES ON USERS.ZIPCODE = ZIPCODES.ZIPCODE WHERE ZIPCODES.CITY = NEW YORK'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'users'
    assert q_res.onop == '='
    assert q_res.onlval == 'users.zipcode'
    assert q_res.onrval == 'zipcodes.zipcode'
    assert q_res.whereop != '>'
    assert q_res.wherelval == 'zipcodes.city'
    assert q_res.whererval == 'NEW YORK'
