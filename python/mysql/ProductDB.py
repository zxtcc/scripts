#i!/usr/local/bin/python
# --*-- coding: gb2312 --*--

import MySQLdb
import sys
import warnings
#import pdb

class ProductDB:

	def __init__(self, dbHost, dbPort, dbUser, dbPasswd, dbName, source):
		'''source indicates where you got the inserted data, it's uesed in importing data to database.
	1 : from EU
	2 : from Data Feed'''
		self.dbHost = dbHost
		self.dbPort = dbPort
		self.dbUser = dbUser
		self.dbPasswd = dbPasswd
		self.dbName = dbName
		self.source = source

	def connect(self):
		self.dbCon = MySQLdb.Connection(host=self.dbHost, port=self.dbPort, user=self.dbUser, passwd=self.dbPasswd, db=self.dbName, client_flag=131072)
		#self.dbCon = MySQLdb.Connection(host=self.dbHost, user=self.dbUser, passwd=self.dbPasswd, db=self.dbName, client_flag=MySQLdb.constants.CLIENT.MULTI_RESULTS)
		self.dbCon.set_character_set('utf8')

	def close(self):
		self.dbCon.close()

	def importArray(self, strArray):
		if len(strArray) != 12:
			return "wrong array size %d" % len(strArray)
		strSql = "call importData("
		for i in range(0,12):
			if i == 1:
				strSql = "%sfrom_unixtime(%s), " % (strSql, self.dbCon.escape_string(strArray[i]) )
			else:
				#try:
				strSql = "%s'%s', "%(strSql, self.dbCon.escape_string(strArray[i]) )
				#except UnicodeEncodeError:
				#pdb.set_trace()
		strSql = strSql + "%d"%self.source + ");"
		return self.executeOne(strSql)

	def executeOne(self, strSql):
		#print strSql;
		#return "test"
		cur = self.dbCon.cursor()
		#with warnings.catch_warnings():
		#warnings.simplefilter("ignore")
		cur.execute(strSql)
		self.dbCon.commit()
		res = cur.fetchone()
		cur.close()
		if res is not None:
			return res[0]
		return None

	def executeOneLine(self,strSql):
		rows=self.executeAll(strSql)
		if len(rows)>0:
			return rows[0]
		return None

	def executeAllWithColumnName(self, strSql):
		#print strSql;
		#return "test"
		cur = self.dbCon.cursor()
		#with warnings.catch_warnings():
		#warnings.simplefilter("ignore")
		cur.execute(strSql)
		if cur.description is None:
			return []
		cn = [ aa[0] for aa in cur.description]
		res = cur.fetchall()[:]
		cur.close()
		return [cn]+ list(res)

	def executeAllUnbuffer(self, strSql):
		cur = self.dbCon.cursor(MySQLdb.cursors.DictCursor)
		cur.execute(strSql)
		return cur

	def executeAll(self, strSql):
		#print strSql;
		#return "test"
		cur = self.dbCon.cursor()
		#with warnings.catch_warnings():
		#warnings.simplefilter("ignore")
		cur.execute(strSql)
		res = cur.fetchall()
		#for data in res
		#	print '%s\t%s' %data
		cur.close()
		return res

	def escapeString(self, str):
		return self.dbCon.escape_string(str)

	def importData(self,
		md,
		crawlTime,
		url,
		title,
		thumbnailDigest,
		thumbnailUrl,
		reviewUrl,
		category,
		description,
		modle,
		price,
		stock
		):
		if md == "" :
			return "error: empty md."
		md = self.dbCon.escape_string(md)
		crawlTime= self.dbCon.escape_string(crawlTime)
		url = self.dbCon.escape_string(url)
		title = self.dbCon.escape_string(title)
		thumbnailDigest = self.dbCon.escape_string(thumbnailDigest)
		thumbnailUrl = self.dbCon.escape_string(thumbnailUrl)
		reviewUrl = self.dbCon.escape_string(reviewUrl)
		category = self.dbCon.escape_string(category)
		description = self.dbCon.escape_string(description)
		modle = self.dbCon.escape_string(modle)
		stock = self.dbCon.escape_string(stock)

		strSql = "call importData('"	\
			+ md + "', '"		\
			+ 'from_unixtime(' + crawlTime + "'), '"	\
			+ url + "', '"		\
			+ title + "', '"	\
			+ thumbnailDigest + "', '"	\
			+ thumbnailUrl + "', '"	\
			+ reviewUrl + "', '"	\
			+ category + "', '"	\
			+ description + "', '"	\
			+ modle + "', '"	\
			+ price + "', '"	\
			+ stock + "', " + "%d"%self.source + ");"
		return self.executeOne(strSql)

def main():
    pdb = ProductDB("0.0.0.0", 0, "user_name", "password", "db_name", 1)
    pdb.connect()
    pdb.close()

if __name__ == "__main__":
	main()
