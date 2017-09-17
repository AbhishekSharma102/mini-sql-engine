import itertools
import loadFiles
import parser
import sys

dbobj = loadFiles.loadDB()

def printIt(data):
	#print len(data['data'])
	for i in data['col']:
		if data['col'].index(i) != len(data['col'])-1:
			print i + ",",
		else:
			print i,
	print ""
	for eachrecord in data['data']:
		counter = 0
		for eachcol in eachrecord:
			if counter == len(eachrecord)-1:
				print str(eachcol),
			else:
				print str(eachcol) + ',',
			counter+=1
		print ""

def checkQuery(columns, fromtables, whereClause, aggfunc, dist):
	schema = dbobj.requestSchema().copy()
	for tables in schema.keys():
		if tables.lower() not in (item.lower() for item in fromtables):
			schema.pop(tables)
	#print schema
	if columns[0] == "*":
		for eachtable in fromtables:
			if eachtable.lower() not in (item.lower() for item in schema.keys()):
				print "Error: " + eachtable.lower() + " not in database"
				return False
		if whereClause:
			whereClause = whereClause[0]
			found = True
			for i in whereClause:
				if type(i) == str and i.lower() not in ["=", "!=", "<", ">", "<=", ">=", "and", "or"] and \
				not (i.isdigit() or (i[0] in ["+", "-"] and i[1:].isdigit())):
					foundquer = False
					for eachtable in fromtables:
						for key in schema.keys():
							if i.lower() in (item.lower() for item in schema[key]):
								foundquer = True
					found = found and foundquer
				elif type(i) != str:
					if not(i[0].lower() in (item.lower() for item in schema.keys()) and\
					 i[2].lower() in (item.lower() for item in schema[i[0].lower()])):
						print "Error: " + "".join(i.asList()) + " doesn't exist"
						return False
			if not found:
				print "Error: Incorrect combination of columns and tables given"
			return found
		else:
			return True
	elif aggfunc:
		found = True
		for i in aggfunc:
			if type(i[2]) == str:
				foundquer = False
				for eachtable in fromtables:
					for key in schema.keys():
						if i[2].lower() in (item.lower() for item in schema[key]):
							foundquer = True
				found = found and foundquer
			else:
				if not(i[2][0].lower() in (item.lower() for item in schema.keys()) and\
				 i[2][2].lower() in (item.lower() for item in schema[i[2][0].lower()])):
					print "Error: " + "".join(i[2].asList()) + "doesn't exist"
					return False
		if whereClause:
			whereClause = whereClause[0]
			found1 = True
			for i in whereClause:
				if type(i) == str and i.lower() not in ["=", "!=", "<", ">", "<=", ">=", "and", "or"] and \
				not (i.isdigit() or (i[0] in ["+", "-"] and i[1:].isdigit())):
					foundquer = False
					for eachtable in fromtables:
						for key in schema.keys():
							if i.lower() in (item.lower() for item in schema[key]):
								foundquer = True
					found1 = found1 and foundquer
				elif type(i) != str:
					if not(i[0].lower() in (item.lower() for item in schema.keys()) and\
					 i[2].lower() in (item.lower() for item in schema[i[0].lower()])):
						print "Error: " + "".join(i.asList()) + " doesn't exist"
						return False
			if not (found and found1):
				print "Error: Incorrect combination of columns and tables given in where clause"
			return found1 and found
		else:
			if not found:
				print "Error: Incorrect combination of columns and tables given for projection"
			return found
	elif dist:
		found = True
		for i in dist:
			if type(i[2]) == str:
				foundquer = False
				for eachtable in fromtables:
					for key in schema.keys():
						if i[2].lower() in (item.lower() for item in schema[key]):
							foundquer = True
				found = found and foundquer
			else:
				if not(i[2][0].lower() in (item.lower() for item in schema.keys()) and\
				 i[2][2].lower() in (item.lower() for item in schema[i[2][0].lower()])):
					print "Error: " + "".join(i[2].asList()) + "doesn't exist"
					return False
		if whereClause:
			whereClause = whereClause[0]
			found1 = True
			for i in whereClause:
				if type(i) == str and i.lower() not in ["=", "!=", "<", ">", "<=", ">=", "and", "or"] and \
				not (i.isdigit() or (i[0] in ["+", "-"] and i[1:].isdigit())):
					foundquer = False
					for eachtable in fromtables:
						for key in schema.keys():
							if i.lower() in (item.lower() for item in schema[key]):
								foundquer = True
					found1 = found1 and foundquer
				elif type(i) != str:
					if not(i[0].lower() in (item.lower() for item in schema.keys()) and\
					 i[2].lower() in (item.lower() for item in schema[i[0].lower()])):
						print "Error: " + "".join(i.asList()) + " doesn't exist"
						return False
			if not (found and found1):
				print "Error: Incorrect combination of columns and tables given in where clause"
			return found1 and found
		else:
			if not found:
				print "Error: Incorrect combination of columns and tables given for projection"
			return found
	else :
		for eachtable in fromtables:
			if eachtable.lower() not in (item.lower() for item in schema.keys()):
				print "Error: " + eachtable.lower() + " not in database"
				return False
		found = True
		for eachitem in columns:
			if type(eachitem) == str:
				foundquer = False
				for eachtable in fromtables:
					for key in schema.keys():
						if eachitem.lower() in (item.lower() for item in schema[key]):
							foundquer = True
				found = found and foundquer
			elif type(eachitem) != str:
				if not(eachitem[0].lower() in (item.lower() for item in schema.keys()) and\
				 eachitem[2].lower() in (item.lower() for item in schema[eachitem[0].lower()])):
					print "Error: " + "".join(eachitem.asList()) + " doesn't exist"
					return False
		if whereClause:
			whereClause = whereClause[0]
			found1 = True
			for i in whereClause:
				if type(i) == str and i.lower() not in ["=", "!=", "<", ">", "<=", ">=", "and", "or"] and \
				not (i.isdigit() or (i[0] in ["+", "-"] and i[1:].isdigit())):
					foundquer = False
					for eachtable in fromtables:
						for key in schema.keys():
							if i.lower() in (item.lower() for item in schema[key]):
								foundquer = True
					found1 = found1 and foundquer
				elif type(i) != str:
					if not(i[0].lower() in (item.lower() for item in schema.keys()) and\
					 i[2].lower() in (item.lower() for item in schema[i[0].lower()])):
						print "Error: " + "".join(i.asList()) + " doesn't exist"
						return False
			#print (found and found1)
			if not (found and found1):
				print "Error: Incorrect combination of columns and tables given in where clause"
			return found and found1
		else:
			if not found:
				print "Error: Incorrect combination of columns and tables given for projection"
			return found

def joinTables(fromtables):
	schema = dbobj.requestSchema().copy()
	tojointables = dbobj.requestDB().copy()
	for tables in schema.keys():
		if tables.lower() not in (item.lower() for item in fromtables):
			schema.pop(tables)
			tojointables.pop(tables)
	for key in tojointables.keys():
		tojointables[key] = tojointables[key].requestTable()
	finalschema = []
	finaltable = []
	for key in tojointables.keys():
		if not finalschema:
			finalschema = [key + "." + item for item in schema[key]]
			finaltable = tojointables[key]
			continue
		finalschema = finalschema + [key + "." + item for item in schema[key]]#key + "." + schema[key]
		temp = []
		for i in finaltable:
			for j in tojointables[key]:
				temp.append(i+j)
		finaltable = temp
	return finaltable, finalschema

def modifywhere(whereClause, schema):
	toreturn = []
	for i in whereClause:
		if type(i) == str:
			if i.lower() in ["=", "!=", "<", ">", "<=", ">=", "and", "or"] or \
				(i.isdigit() or (i[0] in ["+", "-"] and i[1:].isdigit())):
				toreturn.append(i)
			else:
				for key in schema:
					if "."+i.lower() in key.lower():
						toreturn.append(key.lower())
						break
		else:
			toreturn.append("".join((item.lower() for item in i.asList())))
	return toreturn

def getTable(element, schema):
	#print schema
	for key in schema:
		if "."+element.lower() in key.lower():
			return key.lower()

def executeQuer(schema, table, whereClause):
	lhsval = None
	rhsval = None
	lhsnum = None
	rhsnum = None
	temp = [item.lower() for item in schema]
	if whereClause[0].isdigit() or (whereClause[0][0] in ['+', '-'] and whereClause[0][1:].isdigit()):
		lhsval = int(whereClause[0])
	else:
		lhsnum = temp.index(whereClause[0])
	if whereClause[2].isdigit() or (whereClause[2][0] in ['+', '-'] and whereClause[2][1:].isdigit()):
		rhsval = int(whereClause[2])
	else:
		rhsnum = temp.index(whereClause[2])
	newtable = [] 
	if whereClause[1] == "=" and lhsval == None and rhsval == None:
		del schema[rhsnum]
	if lhsval == None and rhsval == None:
		for i in table:
			if whereClause[1] == "=":
				if i[lhsnum] == i[rhsnum]:
					temp = i
					del i[rhsnum]
					newtable.append(temp)
			elif whereClause[1] == "!=":
				if i[lhsnum] != i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == "<":
				if i[lhsnum] < i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == ">":
				if i[lhsnum] > i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == "<=":
				if i[lhsnum] <= i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == ">=":
				if i[lhsnum] >= i[rhsnum]:
					newtable.append(i)
	elif lhsval != None and rhsval == None:
		for i in table:
			if whereClause[1] == "=":
				if lhsval == i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == "!=":
				if lhsval != i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == "<":
				if lhsval < i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == ">":
				if lhsval > i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == "<=":
				if lhsval <= i[rhsnum]:
					newtable.append(i)
			elif whereClause[1] == ">=":
				if lhsval >= i[rhsnum]:
					newtable.append(i)
	elif lhsval == None and rhsval != None:
		for i in table:
			if whereClause[1] == "=":
				if i[lhsnum] == rhsval:
					newtable.append(i)
			elif whereClause[1] == "!=":
				if i[lhsnum] != rhsval:
					newtable.append(i)
			elif whereClause[1] == "<":
				if i[lhsnum] < rhsval:
					newtable.append(i)
			elif whereClause[1] == ">":
				if i[lhsnum] > rhsval:
					newtable.append(i)
			elif whereClause[1] == "<=":
				if i[lhsnum] <= rhsval:
					newtable.append(i)
			elif whereClause[1] == ">=":
				if i[lhsnum] >= rhsval:
					newtable.append(i)
	return schema, newtable

def executeQuerAndOr(schema, table1, table2, operator):
	newtable = []
	if operator.lower() == "and":
		for i in table1:
			if i in table2:
				newtable.append(i)
	else :
		newtable = table1
		for i in table2:
			if i not in newtable:
				newtable.append(i)
	return schema, newtable

def runwhere(table, schema, whereClause):
	whereClause = modifywhere(whereClause[0], schema)
	if len(whereClause) == 3:
		schema, table = executeQuer(schema, table, whereClause)
	elif len(whereClause) == 7:
		schema1, table1 = executeQuer(schema, table, whereClause[0:3])
		schema2, table2 = executeQuer(schema, table, whereClause[4:])
		schema, table = executeQuerAndOr(schema1, table1, table2, whereClause[3])
	else:
		print "Error: Incorrect Condition"
		return schema, table
	return schema, table

def selectAll(table, schema, whereClause):
	if whereClause:
		schema, table = runwhere(table, schema, whereClause)
	toprint = {'data': [], 'col': []}
	toprint['col'] = schema
	toprint['data'] = table
	printIt(toprint)

def calcDist(dist, table, schema, whereClause):
	if whereClause:
		schema, table = runwhere(table, schema, whereClause)
	toprint = {'col':[], 'data': [[] for _ in range(len(table))]}
	for i in dist:
		name = None
		if type(i[2]) == str:
			name = getTable(i[2], schema)
		else:
			name = "".join(i[2].asList())
		templist = [item.lower() for item in schema]
		nameindex = templist.index(name)
		templist = []
		for i in table:
			templist.append(i[nameindex])
		toprint['col'].append("dist(" + name + ")")
		counter = 0
		for i in list(set(templist)):
			toprint['data'][counter].append(i)
			counter += 1
	printIt(toprint)

def calcCols(columns, table, schema, whereClause):
	if whereClause:
		schema, table = runwhere(table, schema, whereClause)
	toprint = {'col':[], 'data': [[] for _ in range(len(table))]}
	for i in columns:
		name = None
		if type(i) == str:
			name = getTable(i, schema)
		else:
			name = "".join(i.asList())
		templist = [item.lower() for item in schema]
		nameindex = templist.index(name)
		toprint['col'].append(name)
		counter = 0
		for i in table:
			toprint['data'][counter].append(i[nameindex])
			counter += 1
	printIt(toprint)

def calcAgg(aggfunc, table, schema, whereClause):
	if whereClause:
		schema, table = runwhere(table, schema, whereClause)
	toprint = {'col': [], 'data': []}
	for i in aggfunc:
		name = None
		if type(i[2]) == str:
			name = getTable(i[2], schema)
		else:
			name = "".join(i[2].asList())
		templist = [item.lower() for item in schema]
		nameindex = templist.index(name)
		if i[0].lower() == "sum":
			sumval = 0
			for i in table:
				sumval += i[nameindex]
			toprint['col'].append("sum("+name+")")
			if not len(table):
				if len(toprint['data']):
					toprint['data'][0].append("-")
				else:
					toprint['data'].append(["-"])
				continue
			if len(toprint['data']):
				toprint['data'][0].append(sumval)
			else:
				toprint['data'].append([sumval])
		elif i[0].lower() == "avg":
			avgval = 0
			for i in table:
				avgval += i[nameindex]
			avgval = float(avgval)/len(table)
			toprint['col'].append("avg("+name+")")
			if not len(table):
				if len(toprint['data']):
					toprint['data'][0].append("-")
				else:
					toprint['data'].append(["-"])
				continue
			if len(toprint['data']):
				toprint['data'][0].append(avgval)
			else:
				toprint['data'].append([avgval])
		elif i[0].lower() == "max":
			maxval = -sys.maxsize
			for i in table:
				maxval = max(maxval, i[nameindex])
			toprint['col'].append("max("+name+")")
			if not len(table):
				if len(toprint['data']):
					toprint['data'][0].append("-")
				else:
					toprint['data'].append(["-"])
				continue
			if len(toprint['data']):
				toprint['data'][0].append(maxval)
			else:
				toprint['data'].append([maxval])
		elif i[0].lower() == "min":
			minval = sys.maxsize
			for i in table:
				minval = min(minval, i[nameindex])
			toprint['col'].append("min("+name+")")
			if not len(table):
				if len(toprint['data']):
					toprint['data'][0].append("-")
				else:
					toprint['data'].append(["-"])
				continue
			if len(toprint['data']):
				toprint['data'][0].append(minval)
			else:
				toprint['data'].append([minval])
	printIt(toprint)



def applyQuery(parsed):
	if parsed[0].lower() == "exit":
		print "Bye!"
		exit(0)
	elif parsed[0].lower() == "select":
		columns = parsed.columns
		fromtables = parsed.fromTables
		whereClause = parsed.conditions
		aggfunc = parsed.special
		dist = parsed.distinct
		if not checkQuery(columns, fromtables, whereClause, aggfunc, dist):
			#print "Table column combination(s) or column(s) do not exist in the given tables"
			return
		joinedTable, joinedSchema = joinTables(fromtables)
		if columns[0] == "*":
			selectAll(joinedTable, joinedSchema, whereClause)
		elif dist:
			calcDist(dist, joinedTable, joinedSchema, whereClause)
		elif aggfunc:
			calcAgg(aggfunc, joinedTable, joinedSchema, whereClause)
		elif columns:
			#print joinedTable
			calcCols(columns, joinedTable, joinedSchema, whereClause)

	elif parsed[0].lower() == "drop":
		print "drop"

'''while(True):
	query = raw_input("MeraSQL> ")
	parsed = parser.parseQuery(query)
	if "where" in query and not parsed.conditions:
		print "Error: Incorrect where clause"
		continue
	if parsed:
		applyQuery(parsed)'''
#print " ".join(sys.argv[1:])
parsed = parser.parseQuery(" ".join(sys.argv[1:]))
if parsed:
	if "where" in " ".join(sys.argv[1:]) and not parsed.conditions:
		print "Error: Incorrect where clause"
	else:
		applyQuery(parsed)