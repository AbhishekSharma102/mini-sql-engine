from pyparsing import *

numbers = Word(nums)
plusorminus = Literal('+') | Literal('-')
intnum = Combine( Optional(plusorminus) + numbers)
exitSt = Keyword("exit", caseless=True)
selectSt = Keyword("select", caseless=True)
exitSt = Keyword("exit", caseless=True)
createSt = Keyword("create", caseless=True)
dropSt = Keyword("drop", caseless=True)
operSt = oneOf("= != > < >= <= eq ne gt lt ge le", caseless=True)
maxSt = Keyword("max", caseless=True)
minSt = Keyword("min", caseless=True)
avgSt = Keyword("avg", caseless=True)
sumSt = Keyword("sum", caseless=True)
fromSt = Keyword("from", caseless=True)
whereSt = Keyword("where", caseless=True)
distinSt = Keyword("distinct", caseless=True)
andSt = Keyword("and", caseless=True)
orSt = Keyword("or", caseless=True)
tableKSt = Keyword("table", caseless=True)
tableSt = Word(alphanums)
colSt = Word(alphanums)
tabColSt = Group(tableSt + "." + colSt)
funcToken = maxSt | minSt | avgSt | sumSt
funcSt = Group(funcToken + '(' + (tabColSt | colSt) + ')')
fundSt = Group(distinSt + '(' + (tabColSt | colSt) + ')')
fromTableSt = Group(delimitedList(tabColSt | tableSt))
whereCond = Forward()
whereCond << ((tabColSt | colSt | intnum) + operSt + (tabColSt | colSt | intnum))
whereExp = Forward()
multWhere = Group(delimitedList(whereCond + Optional((andSt|orSt) + whereCond)))
whereExp << multWhere
colExp = Forward()
colExp << ('*' | Group(delimitedList(fundSt)).setResultsName("distinct") | Group(delimitedList(funcSt)).setResultsName("special") | Group(delimitedList(tabColSt | colSt)))


def parseQuery(query):
	try:
		parser = Forward()
		selSt = Forward().setResultsName('selectStatement')
		selSt << ((selectSt + ('*' | colExp).setResultsName("columns")) + \
			fromSt + fromTableSt.setResultsName("fromTables") + \
			Optional(whereSt + whereExp.setResultsName("conditions")))
		drSt = Forward().setResultsName('dropStatement')
		drSt << (dropSt + tableKSt + tableSt.setResultsName('table'))
		parser = drSt | selSt | exitSt
		tokens = parser.parseString(query)
		return tokens
	except Exception as E:
		print "there seems to be some error in your query!"

'''while(1):
	string = raw_input("> ")
	print parseQuery(string)'''