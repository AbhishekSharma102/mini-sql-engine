import csv
import os
import operator
import re
import sys

class MyDatabase:
	'''
	database has all tables and their objects
	allschema has all table schema
	'''
	def __init__(self):
		self.__database = {}
		self.__allSchema = {}

	def updateDB(self, key, newdb):
		self.__database[key] = newdb

	def updateSchema(self, key, newschema):
		self.__allSchema[key] = newschema

	def requestDB(self):
		return self.__database

	def requestSchema(self):
		return self.__allSchema

class MyTables:
	'''
	table has all rows, is a 2d matrix
	'''
	def __init__(self):
		self.__name = ""
		self.__table = []

	def updateTable(self, newrec):
		self.__table.append(newrec)

	def setName(self, name):
		self.__name = name

	def requestTable(self):
		return self.__table

def loadMetadata(dbobj):
	with open("metadata.txt", 'r') as metafile:
		data = metafile.readlines()
		data = [eachline.strip() for eachline in data]
		tables = {}
		for i in range(len(data)):
			if data[i] == "<begin_table>":
				obj = MyTables()
				key = data[i+1]
				i += 2
				cols = []
				while data[i] != "<end_table>":
					cols.append(data[i])
					i += 1
				dbobj.updateSchema(key, cols)

def loadTables(dbobj, tablename):
	'''table has records in 2d arr as arr[#records][col]'''
	with open(tablename, 'r') as tablefile:
		data = csv.reader(tablefile, delimiter=",")
		records = []
		tabobj = MyTables()
		for row in data:
			for i in range(len(row)):
				row[i] = int(row[i])
			#records.append(row)
			tabobj.updateTable(row)
		dbobj.updateDB(tablename[0:len(tablename)-4], tabobj)
		tabobj.setName(tablename)
		#tabobj.updateTable(records)

def loadDB():
	dbobj = MyDatabase()
	loadMetadata(dbobj)
	for i in dbobj.requestSchema().keys():
		loadTables(dbobj, i + ".csv")
	return dbobj

