""" Accepts the query from the REST Client and parses it find out appropriate conditions based
    on the query template.
"""
from enum import Enum

handles_join = ['select', '*', 'from', 'innerjoin', 'on', 'where']
handles_group = ['select', 'from', 'groupby', 'having']
valid_aggr = ['count', 'min', 'max', 'sum']


class Template(Enum):
    """ENUM class to determine the template of the query
    """
    JOIN = 1
    GROUPBY = 2
    ERROR = 3


class QuerySetJoin:
    """Class to carry query data after parsing JOIN template
    """

    def __init__(self, fromtable, jointable, oncond, wherecond, whererval):
        self.fromtable = fromtable
        self.jointable = jointable
        self.oncond = oncond
        self.onop = None
        self.onlval = None
        self.onrval = None
        self.wherecond = wherecond
        self.whereop = None
        self.wherelval = None
        self.whererval = whererval
        self._processoncond()
        self._processwherecond()

    def _processoncond(self):
        if '=' not in self.oncond:
            # print('On Condition error')
            raise NotImplementedError(
                'On clause operator is undefined. Only "==" is allowed')
        else:
            self.onop = '='
            tmp = self.oncond.split('=')
            self.onlval = tmp[0].lower()
            self.onrval = tmp[1].lower()

    def _processwherecond(self):
        allowed_op = ['<=', '<', '=', '>', '>=']
        if not any(x in self.wherecond for x in allowed_op):
            # print('Where Condition error')
            raise NotImplementedError('Where clause operator is undefined')
        elif '<' in self.wherecond:
            if '<=' in self.wherecond:
                self.whereop = '<='
                tmp = self.wherecond.split('<=')
                self.wherelval = tmp[0].lower()
                # self.whererval = whererval
            else:
                self.whereop = '<'
                tmp = self.wherecond.split('<')
                self.wherelval = tmp[0].lower()
                # self.whererval = whererval
        elif '>' in self.wherecond:
            if '>=' in self.wherecond:
                self.whereop = '>='
                tmp = self.wherecond.split('>=')
                self.wherelval = tmp[0].lower()
                # self.whererval = whererval
            else:
                self.whereop = '>'
                tmp = self.wherecond.split('>')
                self.wherelval = tmp[0].lower()
                # self.whererval = whererval
        elif '=' in self.wherecond:
            self.whereop = '='
            tmp = self.wherecond.split('=')
            self.wherelval = tmp[0].lower()
            # self.whererval = whererval

    def getdata(self):
        return self

    def __str__(self):
        return f"\nFROM: {self.fromtable}\n JOIN: {self.jointable}\n ON: {self.oncond}\n ON-OP: {self.onop}\n ONLVAL: {self.onlval}\n ONRVAL: {self.onrval}\n  WHERE: {self.wherecond}\n WHEREOP: {self.whereop}\n WHERELVAL: {self.wherelval}\n WHERERVAL: {self.whererval}"


class QuerySetGroupBy:
    """Class to carry query data after parsing GROUPBY template
    """

    def __init__(self, selectcol, fromtable, groupcond, havcond):
        self.selectcol = selectcol
        self.selcolumns = None
        self.aggfunc = None
        self.aggcol = None
        self.fromtable = fromtable
        self.groupcond = groupcond
        self.groupcolumns = None
        self.havcond = havcond
        self.havop = None
        self.havlval = None
        self.havrval = None
        self._processelect()
        self._processhaving()
        self._processgroup()
        self._comparegrouphav()

    def _processelect(self):
        if ',' in self.selectcol:
            tmp = self.selectcol.lower().split(',')
            self.selcolumns = tmp
            if '(' in tmp[-1]:
                indexl = tmp[-1].index('(')
                indexr = tmp[-1].index(')')
                self.aggfunc = tmp[-1][:indexl].lower()
                self.aggcol = tmp[-1][indexl+1:indexr].lower()
        else:
            self.selcolumns = self.selectcol
            tmp = self.selcolumns.lower()
            if '(' in tmp:
                indexl = tmp.index('(')
                indexr = tmp.index(')')
                self.aggfunc = tmp[:indexl].lower()
                self.aggcol = tmp[indexl+1:indexr].lower()

    def _processgroup(self):
        if ',' in self.groupcond:
            self.groupcolumns = self.groupcond.lower().split(',')
        else:
            self.groupcolumns = self.groupcond.lower()

    def _processhaving(self):
        allowed_op = ['<=', '<', '=', '>', '>=']
        if not any(x in self.havcond for x in allowed_op):
            raise NotImplementedError('Having clause operator is undefined')
        elif '<' in self.havcond:
            if '<=' in self.havcond:
                self.havop = '<='
                tmp = self.havcond.split('<=')
                self.havlval = tmp[0].lower()
                self.havrval = tmp[1].lower()
            else:
                self.havop = '<'
                tmp = self.havcond.split('<')
                self.havlval = tmp[0].lower()
                self.havrval = tmp[1].lower()
        elif '>' in self.havcond:
            if '>=' in self.havcond:
                self.havop = '>='
                tmp = self.havcond.split('>=')
                self.havlval = tmp[0].lower()
                self.havrval = tmp[1].lower()
            else:
                self.havop = '>'
                tmp = self.havcond.split('>')
                self.havlval = tmp[0].lower()
                self.havrval = tmp[1].lower()
        elif '=' in self.havcond:
            self.havop = '='
            tmp = self.havcond.split('=')
            self.havlval = tmp[0].lower()
            self.havrval = tmp[1].lower()

    def _comparegrouphav(self):
        if not self.selcolumns[-1] == self.havlval:
            raise NameError(
                'Different Aggregate Function in Select clause and Having clause')
        if not self.aggfunc in valid_aggr:
            raise NameError('Not a valid Aggregate Function')

    def getdata(self):
        return self

    def __str__(self):
        return f"\n SELECT {self.selectcol}\n SELECTCOLS {self.selcolumns}\n  FROM {self.fromtable}\n AGGFUNC {self.aggfunc}\n AGGCOL {self.aggcol}\n GROUPBY {self.groupcond}\n GROUPCOL {self.groupcolumns}\n HAVING {self.havcond}\n HAVOP {self.havop}\n HAVLVAL {self.havlval}\n HAVRVAL {self.havrval}"


class Parse:
    """Main Parser class::
    parseQuery(query:str) -> QuerySetJoin or QuerySetGroupBy
    """

    def __init__(self):
        self._whichTemplate = Template.JOIN
        self.query = ""

    def _getWhereRval(self, tmpQuery):
        wRval = ""
        tmpQuery = tmpQuery[::-1]
        for c in tmpQuery:
            if c.isalnum() or c.isspace():
                wRval = wRval + c
            else:
                break
        wRval = wRval[::-1].strip()
        return wRval

    def parseQuery(self, query):
        """Parse the string query into QuerySetJoin or QuerySetGroupBy depending on the
            template of the query
        """
        self.query = query
        tmpQuery = query
        self._decideTemplate()
        self._cleanQuery()
        parsedQuery = None
        query_list = self.query.split()
        
        if self._whichTemplate == Template.JOIN:
            if len(query_list) != 10:
                # print("Error in Join query: Missing arguments")
                raise ValueError('Query Error: Missing Arguments')
            elif len(query_list) == 10:
                if not all(item.upper() in query_list for item in handles_join):
                    # print('Error in Join query: Missing args')
                    raise ValueError('Query Error: Unstructured Query')
                else:
                    fromtable = query_list[3].lower()
                    jointable = query_list[5].lower()
                    oncond = query_list[7]
                    wherecond = query_list[9]
                    whererval = self._getWhereRval(tmpQuery)
                    parsedQuery = QuerySetJoin(
                        fromtable, jointable, oncond, wherecond, whererval)
        elif self._whichTemplate == Template.GROUPBY:
            if len(query_list) != 8:
                # print("Error in Groupby query: Missing arguments")
                raise ValueError('Query Error: Missing Arguments')
            elif len(query_list) == 8:
                if not all(item.upper() in query_list for item in handles_group):
                    # print('Error in Groupby query: Arguments missing')
                    raise ValueError('Query Error: Unstructured Query')
                else:
                    selectcol = query_list[1]
                    fromtable = query_list[3].lower()
                    groupcond = query_list[5]
                    havcond = query_list[7]
                    parsedQuery = QuerySetGroupBy(
                        selectcol, fromtable, groupcond, havcond)
        else:
            # print('Query Error')
            raise ValueError(
                'Query Error: Does not contain InnerJoin or Groupby keyword')
        return parsedQuery

    def _cleanQuery(self):
        self.query = self.query.strip()
        q_strip = self.query.split()
        clean_q_sp = []
        temp = ""

        if self._whichTemplate == Template.JOIN:
            match_handles = handles_join
        elif self._whichTemplate == Template.GROUPBY:
            match_handles = handles_group
        else:
            # print("Query Error")
            raise ValueError(
                'Query Error: Does not contain InnerJoin or Groupby keyword')

        for i, q_sp in enumerate(q_strip):
            if q_sp.lower() not in match_handles:
                temp = temp+q_sp
            else:
                if temp != "":
                    clean_q_sp.append(temp)
                    temp = ""
                clean_q_sp.append(q_sp)
        clean_q_sp.append(temp)
        self.query = (' ').join(clean_q_sp)

    def _decideTemplate(self):
        if 'innerjoin' in self.query.lower():
            self._whichTemplate = Template.JOIN
        elif 'groupby' in self.query.lower():
            self._whichTemplate = Template.GROUPBY
        else:
            self._whichTemplate = Template.ERROR
