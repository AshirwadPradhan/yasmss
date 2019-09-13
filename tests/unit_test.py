import pytest
from yasmss.parsetemplate.parser import Parse


def test_join_normal():
    query = 'SELECT * FROM ZIPCODE INNERJOIN USER ON ZIPCODE.ZIP == USER.ZIP WHERE ZIPCODE.CITY == PILANI'
    q_res = Parse().parseQuery(query=query)
    assert q_res.fromtable == 'zipcode'
    assert q_res.onop == '=='
    assert q_res.onlval == 'zipcode.zip'
    assert q_res.onrval == 'user.zip'
    assert q_res.whereop == '=='
    assert q_res.wherelval == 'zipcode.city'
    assert q_res.whererval == 'pilani'
