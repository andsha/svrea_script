#!/usr/bin/env python3

import sys, os
from urllib.request import urlopen
import time
from hashlib import sha1
import string
import json
import datetime
import logging
import random
import re
from optparse import OptionParser
import shutil

import pgUtil

gCallerId = 'scr06as'
gUniqueKey = '3i0WnjAooYIHhgnyUKF597moNCYnt449kZbK3YAR'

BASE_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
gDBStruct = BASE_FOLDER + '/svrea/DBStruct.cfg'
gDBFillRules = BASE_FOLDER + '/svrea/DBFillRules.cfg'

gLimit = 3

UPDATE = 1000
INSERT = 1001

CREATE = 2000
DELETE = 2001
TYPE = 2002

LISTINGS = 3001
SOLD = 3002
LASTSOLD = 3003
LASTSOLD = 3003


def err(obj = None, msg = None):

        if msg is not None:
            logging.error(msg)

        if obj is not None:
            obj.pgcon.close()

            try:
                obj.error += 1
            except NameError:
                return None
            else:
                return obj.error

class DataBase():

    def __init__(self, connection, fdbstruct, frules):
        self.pgcon = connection
        self.fdbstruct = fdbstruct # file with version 0 DB structure
        self.frules = frules       # file with filling rules

        self.schema = self.getSchema()
        self.version = self.getVersion()

        self.tableDic = self.getTableDic()
        self.primaryKeys = self.getPrimaryKeys()
        self.foreignKeys = self.getForeignKeys()
        self.error = 0
        self.date = ''

        self.source = None

        #print("select", self.pgcon.run("SELECT 10"))


    def getSchema(self,fdbstruct = None):
        if fdbstruct is None:
            fdbstruct = self.fdbstruct

        jsonData = open(self.fdbstruct).read()
        dbstruct = json.loads(jsonData)

        schema = dbstruct['SCHEMANAME']
        return schema


    #   Get current version of schema
    def getVersion(self):
        sql = """SELECT 1 FROM information_schema.tables
                  WHERE table_schema = '%s'
                  AND table_name = 'info' """ % self.schema

        res = self.pgcon.run(sql, True)

        if len(res) == 0:
            return None

        sql = """SELECT version FROM "%s".info where date = (SELECT MAX(date) FROM "%s".info)""" % (self.schema, self.schema)
        res = self.pgcon.run(sql, True)
        return res[0][0]


    def getTableDic(self):
        sql = """SELECT 1 FROM information_schema.tables
                  WHERE table_schema = '%s'
                  AND table_name = 'info' """ % self.schema

        res = self.pgcon.run(sql, True)

        if len(res) == 0:
            return {}

        sql = """SELECT tabledic FROM "%s".info where version = %s""" % (
        self.schema, self.version)
        res = self.pgcon.run(sql, True)
        return res[0][0]


    def getPrimaryKeys(self):
        sql = """SELECT 1 FROM information_schema.tables
                  WHERE table_schema = '%s'
                  AND table_name = 'info' """ % self.schema

        res = self.pgcon.run(sql, True)

        if len(res) == 0:
            return []

        sql = """SELECT primarykey FROM "%s".info where version = %s""" % (
            self.schema, self.version)
        res = self.pgcon.run(sql, True)
        return res[0][0].split(',')


    def getForeignKeys(self):
        sql = """SELECT 1 FROM information_schema.tables
                  WHERE table_schema = '%s'
                  AND table_name = 'info' """ % self.schema

        res = self.pgcon.run(sql, True)

        if len(res) == 0:
            return {}

        sql = """SELECT foreignkey FROM "%s".info where version = %s""" % (
            self.schema, self.version)
        res = self.pgcon.run(sql, True)
        return res[0][0]





    def updateDB(self, fdbstruct = None, toVersion = -1, startOver = False):

        if fdbstruct is None:
            fdbstruct = self.fdbstruct
        else:
            self.fdbstruct = fdbstruct

        if toVersion == -1:
            toVersion = self.getMaxVersion(fdbstruct)

        jsonData = open(fdbstruct).read()
        dbstruct = json.loads(jsonData)

        if 'SCHEMANAME' not in dbstruct:
            return err("SCHEMANAME could not be found in config")

        schema = dbstruct['SCHEMANAME']
        self.schema = schema

        self.version = self.getVersion()
        fromVersion = self.version

        if self.version is None or startOver is True:
            fromVersion = -1

        for version in range(fromVersion, toVersion):
            updversion = version + 1

            #print ('updversion', updversion)

            if updversion == 0: # new DB
                logging.info('drop schema and start over')
                sql = "DROP SCHEMA IF EXISTS %s CASCADE" % schema
                res = self.pgcon.run(sql)

                if res != 0:
                    return err("Error while dropping schema %s" % schema)

                logging.info("Creating SCHEMA %s" % schema)
                sql = "CREATE SCHEMA %s;" % schema
                res = self.pgcon.run(sql)

                if res != 0:
                    return err("Error while creating schema %s" % schema)

                self.tableDic = {}
                self.primaryKeys = []
                self.foreignKeys = {}

            dbstruct = self.getDBStruct(uversion = updversion)
            #print (updversion, dbstruct)
            #foreignKeys = {}
            #primaryKeys = []

            if 'TABLES' not in dbstruct:
                continue

            for t in dbstruct['TABLES']:
                if 'NAME' not in t:
                    return err('No table name provided in %s. Table name should be called "NAME"' % t)

                table = t['NAME']

                #print (self.tableDic)
                if 'COLUMNS' in t:
                    if table in self.tableDic:
                        sql = """ALTER TABLE "%s"."%s" """ %(schema,table)


                        for c in t['COLUMNS']:
                            column = c['NAME']

                            if column in self.tableDic[table]:
                                if 'ACTION' in c and c['ACTION'] == 'DELETE':
                                    sql += """DROP "%s", """ %column
                                    self.tableDic[table].pop(column)

                                    if "%s.%s" %(table, column) in self.foreignKeys:
                                        self.foreignKeys.pop('%s.%s' %(table,column))

                                    if "%s.%s" %(table, column) in self.primaryKeys:
                                        self.primaryKeys.remove("%s.%s" %(table, column))

                                elif 'ACTION' in c and c['ACTION'] != 'TYPE':
                                    logging.error("undefined column operation")
                                else:
                                    ctype = c['TYPE']
                                    sql += """ALTER "%s" TYPE %s, """ %(column, ctype)
                                    self.tableDic[table][column] = ctype
                            elif 'ACTION' in c and c['ACTION'] == 'CREATE' or 'ACTION' not in c:
                                ctype = c['TYPE']
                                sql += """ADD "%s" %s  """ %(column, ctype)
                                self.tableDic[table][column] = ctype

                                if 'CONSTRAINT' in c:
                                    if c['CONSTRAINT'] == 'PRIMARY KEY':
                                        sql += ' CONSTRAINT %s PRIMARY KEY ' % (t['NAME'] + 'key')
                                        self.primaryKeys.append('%s.%s' % (table, column))
                                    else:
                                        sql += ' %s' % c['CONSTRAINT']

                                sql += ','

                            if 'FOREIGN KEY' in c:
                                fkey = c['FOREIGN KEY']
                                self.foreignKeys['%s.%s' % (table, column)] = fkey

                        sql = sql.rstrip(', ')

                    else:
                        sql = """CREATE TABLE "%s"."%s"(""" % (schema, table)
                        self.tableDic["%s" % table] = {}

                        for c in t['COLUMNS']:
                            column = c['NAME']
                            #print (table, column)
                            ctype = c['TYPE']
                            sql += ' "%s" %s ' % (column, ctype)
                            self.tableDic["%s" % table]["%s" % column] = ctype

                            if 'FOREIGN KEY' in c:
                                fkey = c["FOREIGN KEY"]
                                self.foreignKeys["%s.%s" % (table, column)] = fkey

                            if 'CONSTRAINT' in c:
                                if c['CONSTRAINT'] == 'PRIMARY KEY':
                                    sql += ' CONSTRAINT %s PRIMARY KEY ' % (t['NAME'] + 'key')
                                    self.primaryKeys.append('%s.%s' % (table, column))
                                else:
                                    sql += ' %s' % c['CONSTRAINT']
                            sql += ','
                        sql = sql.rstrip(', ')
                        sql += ')'

                    #print (sql)
                    res = self.pgcon.run(sql)

                    if res != 0:
                        return err("Error while creating table %s%s" % (schema, t))

                if 'INDEXES' in t:
                    for i in t['INDEXES']:
                        idxname = ''
                        if "NAME" in i:
                            idxname = i['NAME']
                        else:
                            idxname = "%sidx" % i['COLUMNS'][0]

                        sql = """CREATE INDEX  %s ON "%s"."%s" (""" % (idxname, schema, table)

                        if 'COLUMN' not in i:
                            return err("No information about indexes in config")

                        for col in i['COLUMN']:
                            sql += '"%s",' % col

                        sql = sql.rstrip(',')
                        sql += ')'


                        res = self.pgcon.run(sql)

                        if res != 0:
                            return err("Error while creating index %s" % idxname)

            for fkey in self.foreignKeys:  # only one level of dependence
                if self.foreignKeys[fkey] in self.foreignKeys:
                    return err("Interdependent Foreign Key '%s' : '%s'" % (fkey, self.foreignKeys[fkey]))

            #self.foreignKeys = foreignKeys
            #self.primaryKeys = primaryKeys

            #print('primaryKeys:', self.primaryKeys)
            #print('foreignKeys:', self.foreignKeys)
            #print('tableDic', self.tableDic)

            # create auxilary tables

            sql = """CREATE TABLE IF NOT EXISTS "%s"."info" (version    int,
                                                            date        timestamp,
                                                            tabledic    json,
                                                            foreignkey  json,
                                                            primarykey  varchar(2000))""" % schema
            res = self.pgcon.run(sql)

            sql = """INSERT INTO "%s".info VALUES (%s,'%s', '%s', '%s', '%s')
                  """ %(schema, updversion,datetime.datetime.now(), ('%s' %self.tableDic).replace("'",'"'), ('%s' %self.foreignKeys).replace("'", '"'), ",".join(self.primaryKeys))
            res = self.pgcon.run(sql)

            sql = """CREATE TABLE IF NOT EXISTS "%s".history   (id     int PRIMARY KEY,
                                                                date    timestamp DEFAULT now(),
                                                                script  varchar(200),
                                                                status  varchar(2000))""" %self.schema
            res = self.pgcon.run(sql)





    def fillDB(self, frules = None, source = None):

        if frules is None:
            frules = self.frules
        else:
            self.frules = frules

        jsonData = open(frules).read()
        rules = json.loads(jsonData)

        # Call creteDB to create/update foreignKeys, primaryKeys, tableDic
        #self.updateDB(updateRulesOnly = True)

        if source is None:
            return err("No source file is given")

        self.source = source
        jsonData = open(source).read()
        data = json.loads(jsonData)
        #print (data)
        #exit()

        if 'listings' in data:
            data = data['listings']
        elif 'sold' in data:
            data = data['sold']
        else:
            return err('file %s contains neither "listings" nor "sold" keys' %source)
        #-----------------------------------------------


        for dic in data: # individual data entry from json library
            valueDic = {}
            uniqueDic = {}
            skipTable = []
            skipColumn = []
            #print("________another listing________")

            for table in self.tableDic: # for each table from list of all tables
                valueDic[table] = {}

                for column in self.tableDic[table]: # for each column in table
                    #print ('\n',table, column)
                    ctype = self.tableDic[table][column]

                    if '%s.%s' %(table, column) in rules: # if rule for this column exists in rules dictionary

                        rule = rules['%s.%s' %(table, column)] # get rule

                        # _________________ working with column keywords _______________

                        keylist = rule.split('##')  # search for keywords
                                                    # rule has following form:
                                                    # "_KEYWORD_##KEYVALUE1##KEYVALUE2##...##PATH"
                                                    # where _KEYWORD_ is always first
                        #print("#1", plist)

                        keyword = None
                        keyvalue = None

                        if len(keylist) != 1:
                            keyword = keylist[0] # always first
                            keyvalue = keylist[1:] # anything in between keyword and path
                            #path = keylist[-1] # always last
                        else:
                            keyvalue = keylist

                        value = ''
                        #print(keyword, keyvalue)

                        if keyword is None: # if no keywords, then get value according to rule
                            value = self.getDicValue(dic, keyvalue[0])
                            #print(type(value))
                            if type(value) is str:
                                value = value.replace("'", "''")

                        elif keyword == '_REGEX_':
                            st = self.getDicValue(dic, keyvalue[-1])
                            #print(st)

                            if st is None:
                                value = ''
                            else:
                                s = re.search(keyvalue[0],st)
                                #print(st, s)

                                if s is not None:
                                    value = s.group(1)
                                else:
                                    value = ''

                        elif keyword == '_REGEX_REP_':
                            st = self.getDicValue(dic, keyvalue[-1])

                            if st is None:
                                value = ''
                            else:
                                s = re.sub(keyvalue[0], '', st)

                                if s is not None:
                                    value = s
                                else:
                                    value = ''

                        elif keyword == '_UNIQUE_NUMBER_':
                            if '%s.%s' %(table,column) not in uniqueDic:
                                uniqueDic['%s.%s' %(table,column)] = []

                            for col in keyvalue[0].split(','):
                                uniqueDic['%s.%s' %(table,column)].append(col)

                        elif keyword == '_IFEXISTS_':
                            #print (keyvalue)
                            value = self.getDicValue(dic, keyvalue[0])

                            #print (value)
                            if value is None:
                                (val, specialKey) = self.checkKey(keyvalue[2])
                            else:
                                if keyvalue[1] == '':
                                    val = value
                                    specialKey = True
                                    #print(val)
                                else:
                                    (val, specialKey) = self.checkKey(keyvalue[1])

                            if specialKey is True:
                                value = val
                            else:
                                value = self.getDicValue(dic, val)

                            #print(value)

                        elif keyword == '_SKIPCOLUMN_':
                            skipColumn.append('%s.%s' %(table, column))

                        else: #default
                            logging.warning('Unknown keyword %s' %keyword)
                            value = None
                    else:
                        value = None # if no rule exists in dictionary for this column, value is None

                    #print(table, column, value, rule)

                    if value is None:
                        if ctype == 'int' or 'decimal' or 'boolean' or 'serial':
                            value = 'NULL'
                        else:
                            value = ''

                    valueDic[table][column] = value


                    #print(self.primaryKeys)
                    #print(table,column)

            # treat Unique constraints ____________________________________________

            #print("Unique Dic:",uniqueDic)

            for tab in uniqueDic:
                [t,c] = tab.split('.')

                sql = """SELECT "%s" FROM "%s"."%s" WHERE""" %(c,self.schema,t)

                for col in uniqueDic[tab]:
                    val = valueDic[t][col]
                    sql += """ "%s" %s AND""" %(col,("='%s'" %val) if val !='NULL' else 'is NULL')

                sql = sql[:-4]
                res = self.pgcon.run(sql,True)

                #print(sql)
                #print(res)

                if len(res) == 0:
                    sql = """SELECT max("%s") FROM "%s"."%s" """ %(c,self.schema,t)
                    res = self.pgcon.run(sql, True)

                    if res[0][0] is None:
                        value = 1
                    else:
                        value = res[0][0] + 1
                else:
                    value = res[0][0]

                #print ("idx", idx)
                #print('value2',value)
                valueDic[t][c] = value

            #print('valueDic', valueDic)

            # treat Foreign Keys _________________________________________________

            #print('foreigh keys:', self.foreignKeys)
            for fkey in self.foreignKeys:
                table, column = fkey.split('.')
                ftable, fcolumn = self.foreignKeys[fkey].split('.')

                if ftable in valueDic and fcolumn in valueDic[ftable]:
                    valueDic[table][column] = str(valueDic[ftable][fcolumn])
                else:
                    return err("Missing foreign table of column '%s' : '%s'" %(ftable, fcolumn), self)

            #__Treat table keywords_____________________________________________________________________

            for table in valueDic:
                if table in rules:
                    #print(table, 'in rules')
                    rule = rules[table]
                    keylist = rule.split('##')  # search for keywords
                    keyword = None
                    keyvalue = None

                    if len(keylist) != 1:
                        keyword = keylist[0]
                        keyvalue = keylist[1:]
                        #path = keylist[-1]
                    else:
                        keyvalue = keylist

                    # this keyword is booli-specific.
                    if keyword == '_SKIPIFSAMEASLATEST_':
                        value = valueDic[table][keyvalue[0]]
                        sql = """SELECT id, "%s", "%s", "%s", "%s" FROM "%s"."%s" WHERE  "%s" %s ORDER BY "%s" DESC, id DESC
                                 """ %(keyvalue[0],keyvalue[1], keyvalue[2], keyvalue[3], self.schema,table, keyvalue[0], 'is NULL' if value == 'NULL' else " = '%s'" %value, keyvalue[2])
                        res = self.pgcon.run(sql, True)

                        if len(res) != 0: # if we have at least one price entry for this booliId
                            value = valueDic[table][keyvalue[1]]
                            skip = True

                            if value != 'NULL':
                                #print((valueDic[table][keyvalue[0]], valueDic[table][keyvalue[1]], valueDic[table][path]) in res)
                                #for r in res:
                                    #if [valueDic[table][keyvalue[0]], valueDic[table][keyvalue[1]], valueDic[table][path]]  ==

                                for r in res:
                                    #print(r, value)
                                    #print(r , datetime.datetime.strptime(valueDic[table][keyvalue[2]].split()[0],"%Y-%m-%d" ), value)

                                    # if this price appears earlier than price from priceHistory
                                    if r[3] > datetime.datetime.strptime(valueDic[table][keyvalue[2]].split()[0],"%Y-%m-%d" ):
                                        skip = True
                                        break

                                    # if price from priceHistory is None get next price
                                    if r[2] is None:
                                        continue
                                    else:
                                        #print (str(r[2]), str(valueDic[table][path]), str(r[2]) == valueDic[table][path] )
                                        # if this price equals to price from priceHistory
                                        if r[2] == value:
                                            c = valueDic[table][keyvalue[3]]
                                            curSoldPrice = True if c == 'True' else False

                                            #print(r[3], r[3] == curSoldPrice)

                                            if r[4] == curSoldPrice:
                                                skip = True
                                            else:
                                                skip = False
                                            break
                                        else:
                                            #print(r, res)
                                            #print(str(r[2]), str(valueDic[table][path]))
                                            skip = False
                                            break

                            if skip:
                                skipTable.append((table))

                        # if new entry for this booliId, apply it to the table


            #____FILL VALUES____________________________________________________________

            #print('ValueDic',valueDic)
            #print (self.primaryKeys)

            for table in valueDic:

                if table in skipTable:
                    continue

                for column in valueDic[table]:
                    if '%s.%s' % (table, column) in skipColumn:
                        continue

                    uoi = INSERT
                    #print (table, column)
                    if '%s.%s' %(table,column) in self.primaryKeys:
                        #print (table, column)
                        value = valueDic[table][column]

                        sql = """SELECT * FROM "%s"."%s" WHERE "%s" = '%s' """ % (self.schema, table, column, value)
                        res = self.pgcon.run(sql, True)
                        # if this pkey already exists in table, rewrite it
                        if len(res) != 0:
                            #print (res)
                            uoi = UPDATE
                            pkcol = column
                            pkval = value

                        break

                if uoi == UPDATE:
                    sql = """UPDATE "%s"."%s" SET""" %(self.schema,table)

                    for column in valueDic[table]:
                        value = valueDic[table][column]
                        sql += """ "%s" = %s,""" %(column, 'NULL' if value=='NULL' else "'%s'" %value)

                    sql = sql[:-1] + """ WHERE "%s" = '%s' """ %(pkcol, pkval)

                    #if table =='listings':
                     #   print(sql)

                elif uoi == INSERT:
                    sql = """INSERT INTO "%s"."%s" ( """ % (self.schema, table)

                    for column in valueDic[table]:

                        if '%s.%s' % (table, column) in skipColumn:
                            continue

                        sql += '"%s", ' % column

                    sql = sql[:-2] + ') VALUES ('

                    for column in valueDic[table]:
                        value = valueDic[table][column]

                        if '%s.%s' % (table, column) in skipColumn:
                            continue

                        sql += "%s," % ('NULL' if value == 'NULL' else "'%s'" % value)

                    sql = sql[:-1] + ')'

                res = self.pgcon.run(sql)

                #if valueDic['priceHistory']['booliId'] == '2098509':
                #print (sql)

                if res != 0:
                    return err("Error while inserting data into %s.%s\n%s" %(self.schema,table, valueDic[table]))

                self.updateTable(table, valueDic)


    #     Table specific rules which cannot be described by the rules
    def updateTable(self, table, valueDic):
        #print(valueDic['listings']['dateSold'])
        if table == 'listings':
            #print (valueDic['listings']['dateSold'])

            #   set isActive and dateinactive
            if valueDic['listings']['dateSold'] == 'NULL':
                sql = """UPDATE "%s"."%s" SET "isActive" = 'True', "dateInactive" = NULL
                          WHERE "booliId" = %s """ %(self.schema, table, valueDic['listings']['booliId'])
            else:
                sql = """UPDATE "%s"."%s" SET "isActive" = 'False', "dateInactive" = '%s'
                    WHERE "booliId" = %s """ % (self.schema, table, valueDic['listings']['dateSold'], valueDic['listings']['booliId'])
                #print('dateInactive:',self.date)
            self.pgcon.run(sql)



    def getDicValue(self,dic,s):
        l = s.split(':')

        if len(l) == 1:
            if s in dic:
                res = dic[s]

                if type(res) is list:
                    return ','.join(res)
                else:
                    return res
            else:
                return None
        else:
            if l[0] in dic:
                return self.getDicValue(dic[l[0]],':'.join(l[1:]))
            else:
                return None


    def checkKey(self, key):
        val = key
        specialKey = False

        if key == '_DATEFROMFILENAME_':
            specialKey = True
            fname = self.source.split('/')[-1]
            # print(fname)
            if fname[:5] == 'booli':
                val = fname.split()[1]

        if key == '_FALSE_':
            specialKey = True
            val = 'False'

        if key == '_TRUE_':
            specialKey = True
            val = 'True'

        return (val, specialKey)



    #   Get maximum update version from DBStruct configs

    def getMaxVersion(self, fdbstruct = None):
        if fdbstruct is None:
            fdbstruct = self.fdbstruct

        jsonData = open(fdbstruct).read()
        dbstruct = json.loads(jsonData)
        maxU = 0

        for u in dbstruct['UPDATES']:
            if int(u) > maxU:
                maxU = int(u)

        val = dbstruct['UPDATES'][str(maxU)]

        if type(val) is dict:
            return maxU
        elif type(val) is str:
            key,path = val.split('##')
            if key == 'FILE':
                return self.getMaxVersion(path)
            else:
                return maxU - 1
        else:
            return err('Unknown update method %s' %dbstruct['UPDATES'])


    #    Get db structure dictionary for given update version

    def getDBStruct(self, uversion, fdbstruct = None):
        if fdbstruct is None:
            fdbstruct = self.fdbstruct

        jsonData = open(fdbstruct).read()
        dbstruct = json.loads(jsonData)
        maxU = 0

        for u in dbstruct['UPDATES']:
            if int(u) == uversion and type(dbstruct['UPDATES'][u]) is dict:
                return dbstruct['UPDATES'][u]
            if int(u) > maxU:
                maxU = int(u)

        #print (type(dbstruct['UPDATES'][str(maxU)]))
        if type(dbstruct['UPDATES'][str(maxU)]) is str:
            key,path = dbstruct['UPDATES'][str(maxU)].split('##')
            if key == 'FILE':
                return self.getDBStruct(uversion, path)
        else:
            return err("Cannot find requested version")



    # ---------------   This is booli-specific function to prepare for data load ---------------

    def initFill(self):
        sql = """UPDATE  "%s"."%s" SET "isActive" = 'False', "dateInactive" = '%s' WHERE "dateSold" is NULL AND "dateInactive" is NULL """ %(self.schema, 'listings', self.date)
        self.pgcon.run(sql)

    def getDataFromWeb(self, type=LISTINGS, latest = False):
        uniqueString = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(16))
        timestamp = str(int(time.time()))
        hashstr = sha1((gCallerId + timestamp + gUniqueKey + uniqueString).encode('utf-8')).hexdigest()
        urlBase = 'HTTP://api.booli.se//'

        if type == LISTINGS:
            urlBase += 'listings?'
        elif type == SOLD or type == LASTSOLD:
            urlBase += 'sold?'
        else:
            return err('Wrong API type %s' % type)

        area_list = ['64',  # Skane
                     '160',  # Halland
                     '23',  # vastra gotalands
                     '2',  # Stockholm lan
                     '783',  # Kronobergs lan
                     '45',  # Blekinge lan
                     '381',  # Kalmar
                     '153',  # Jonkoping
                     '253',  # ostergotland lan
                     '26',  # sodermanlands lan
                     '145',  # Blekinge lan
                     ]  #
        # area = '64'
        for idx, area in enumerate(area_list):
            logging.info("Donwloading for area %s" % (area))
            url = urlBase + \
                  'areaId=' + area + \
                  '&callerId=' + gCallerId + \
                  '&time=' + timestamp + \
                  '&unique=' + uniqueString + \
                  '&hash=' + str(hashstr)
            # print (url)
            # exit()
            data = urlopen(url).read().decode('utf-8')
            dic = json.loads(data)
            maxcount = int(dic['totalCount'])
            # print (maxcount)

            if type == LASTSOLD:
                maxcount = 300

            offset = 0
            limit = 300

            while 1:
                logging.info("%s out of %s" % (offset / limit + 1, int(maxcount / limit) + 1))
                uniqueString = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(16))
                timestamp = str(int(time.time()))
                hashstr = sha1((gCallerId + timestamp + gUniqueKey + uniqueString).encode('utf-8')).hexdigest()
                url = urlBase + \
                      'areaId=' + area + \
                      '&offset=' + str(offset) + \
                      '&limit=' + str(limit) + \
                      '&callerId=' + gCallerId + \
                      '&time=' + timestamp + \
                      '&unique=' + uniqueString + \
                      '&hash=' + str(hashstr)
                # print (url)
                data = urlopen(url).read().decode('utf-8')
                dic = json.loads(data)
                # today = datetime.datetime.today()
                #data = '{"data" : 23}'
                #print('listings' if type == LISTINGS else 'sold', area, data.replace("'", '"'))
                sql = """
                    INSERT INTO "%s".raw_data (download_date,
                                          type,
                                          countyid,
                                          listings_data)
                    VALUES
                    (                     now(),'%s',%s,'%s')
                """ % (self.schema,'listings' if type == LISTINGS else 'sold', area, ('%s' %dic).replace('True', "'True'").replace("'", '"'))
                #print(sql)
                res = self.pgcon.run(sql)
                # fname = BASE_FOLDER + '/data/booli '+ str(datetime.datetime(today.year, today.month, today.day, today.hour, today.minute, today.second)).replace(':','_') + ' ' + area
                # f = open(fname, 'w')
                # json.dump(dic, f)

                # f.close()
                offset += limit

                if offset >= maxcount:
                    logging.info("Downloading complete")
                    break
                if latest:
                    logging.info("Downloading complete")
                    break

                time.sleep(random.randint(15, 30))

            if idx < len(area_list) - 1:
                time.sleep(random.randint(15, 30))




class svrea():

    def __init__(self):
        self.transfer = False
        self.downloadAction = None
        self.updateVersion = None
        self.recreate = False
        self.mcommand = None
        self.clean = False
        self.latest = False
        self.force = False
        self.options = {}
        self.config = None


    def handleCmdLine(self):
        usage = ("%prog <options> ...\n")
        parser = OptionParser(usage = usage)

        # parser.add_option("-d",
        #                   "--download",
        #                   type = "string",
        #                   help = "To download data from internet. 'listings' or 'sold'")
        #
        # parser.add_option("-u",
        #                   "--update",
        #                   type="int",
        #                   help="Update database until version. -1 is latest possible version")
        #
        # parser.add_option("-t",
        #                   "--transfer",
        #                   action="store_true",
        #                   default = False,
        #                   help="Fill database with new data")
        #
        # parser.add_option("-c",
        #                   "--clean",
        #                   action="store_true",
        #                   default=False,
        #                   help="Move downloaded files from data to dataHistory folder. Performed after uploading to database when used with -f.")
        #
        # parser.add_option("-r",
        #                   "--recreate",
        #                   action="store_true",
        #                   default = False,
        #                   help="Clear and recreare database. Use only with -u")
        #
        # parser.add_option("-l",
        #                   "--latest",
        #                   action="store_true",
        #                   default=False,
        #                   help="Download only latest data (latest 300 entries). use with -d")
        #
        # parser.add_option("-f",
        #                   "--force",
        #                   action="store_true",
        #                   default=False,
        #                   help="Forse script execution")

        parser.add_option("-c",
                          "--config",
                          type="string",
                          help="configuration file")

        (options,args) = parser.parse_args()
        self.options = ""

        # if options.download == 'listings' or options.download == 'listing':
        #     self.options += '-d listings '
        #     self.downloadAction = LISTINGS
        # elif options.download == 'sold':
        #     self.options += '-d sold '
        #     self.downloadAction = SOLD
        # elif options.download == 'lastsold':
        #     self.downloadAction = LASTSOLD

        if options.config is not None:
            self.config = options.config
        else:
            err(msg='no config was supplied')
            return 1

        # if options.latest:
        #     self.latest = True
        #     self.options += '-l '
        #
        # if options.update is not None:
        #     self.updateVersion = options.update
        #     self.options += '-u %s ' %self.updateVersion
        #
        # if options.transfer is True:
        #     self.transfer = True
        #     self.options += '-t '
        #
        # if options.force is True:
        #     self.force = True
        #     self.options += '-f '
        # #else:
        # #    return err(msg = "Action should be either update -u,  download -d, or fill -f")
        #
        # if options.recreate:
        #     if options.update is not None:
        #         self.recreate = True
        #         self.options += '-r '
        #     else:
        #         return err(msg = "-r option can only be used with -u")
        #
        # self.clean = options.clean
        # if self.clean:
        #     self.options += '-c'

        return 0




    def run(self):

        FORMAT = '%(levelname)-8s %(asctime)-15s %(message)s'
        logging.basicConfig(level = logging.DEBUG, format=FORMAT)

        if self.handleCmdLine() != 0:
            return 1

        print (self.config)
        return 0


        with open(self.config) as param_file:
            params = json.load(param_file)

        bparams = params["default"]
        db = DataBase(connection = pgUtil.pgProcess(database = bparams['database'], host = bparams['host'], port = bparams['port'], user = bparams['user'], password = bparams['password']), fdbstruct = gDBStruct, frules = gDBFillRules)

        sql = """SELECT 1 FROM information_schema.tables
                  WHERE table_schema = '%s'
                  AND table_name = 'history' """ % db.schema
        #print("options", self.options)

        res = db.pgcon.run(sql, True)

        if len(res) != 0:
            if self.downloadAction == LISTINGS:
                s = 'listings'
            elif self.downloadAction == SOLD:
                s = 'sold'
            else:
                s = ''

            today = datetime.datetime.today().date()
            sql = """SELECT id, status from "%s".history WHERE "date"::date = '%s' ORDER BY date DESC""" %(db.schema, today)
            res = db.pgcon.run(sql, True)

            #print(res)

            #print (res[0][1], not self.make)
            runbefore = False
            for r in res:
                if len(res) != 0 and r[1] == '%s done' %s and not self.force:
                    logging.info("already run today for %s" %s)
                    return 0
                elif len(res) != 0 and r[1] == '%s started' %s:
                    id = int(r[0])
                    runbefore = True
                    break

            if not runbefore:
                sql = """SELECT max(id) from "%s".history """ %db.schema
                res = db.pgcon.run(sql, True)
                id = int(res[0][0])

            if not self.force:
                sql = """INSERT INTO "%s".history values(%s, now(), '%s', '%s started') """ % (
                db.schema, id + 1, self.options, s)
                # print (sql)
                res = db.pgcon.run(sql)
        else:
            id = 0

        # -----------------------------------------------------------------------------------

        if self.updateVersion is not None:
            version = db.getMaxVersion() if self.updateVersion == -1 else self.updateVersion

            if self.recreate:
                logging.info("Recreating Database to version %s" %version)
            else:
                logging.info("Updating Database from version %s to version %s" % (db.version, version))

            db.updateDB(toVersion = self.updateVersion, startOver = self.recreate)

        # -----------------------------------------------------------------------------------

        if self.downloadAction is not None:
            logging.info("Downloading %s" %('listings' if self.downloadAction == LISTINGS else 'sold'))
            db.getDataFromWeb(type = self.downloadAction, latest = self.latest)
        # -----------------------------------------------------------------------------------

        if self.transfer:
            logging.info("Uploading data")


            flist = os.listdir(BASE_FOLDER + '/data/')
            idx = 0

            while flist[idx][:5] != 'booli':
                idx += 1

            db.date = (datetime.datetime.strptime(flist[idx].split()[1], "%Y-%m-%d") - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")
            #print(db.date)
            db.initFill()

            for idx, file in enumerate(flist):
                if file[:5] == 'booli':
                    # logging.info("Uploading file %s %s/%s" %(file,idx,len(flist)))
                    logging.info("Uploading file %s %s/%s" % (file, idx + 1, len(flist)))
                    db.fillDB(source=BASE_FOLDER + '/data/' + file)

        # -----------------------------------------------------------------------------------

        if self.clean:
            logging.info("Cleaning data folder")
            flist = os.listdir(BASE_FOLDER + '/data/')

            if flist is not None:
                for file in flist:
                    #print(file)
                    print(BASE_FOLDER + '/data/' + file, BASE_FOLDER + '/dataHistory/' + file)
                    shutil.copyfile('' + BASE_FOLDER + '/data/' + file + '', '' + BASE_FOLDER + '/dataHistory/' + file + '')
                    os.remove(BASE_FOLDER + '/data/' + file)

        if self.recreate:
            sql = sql = """INSERT INTO "%s".history values(%s,now(), '%s', 'DB created') """ %(db.schema, 1, self.options)
            res = db.pgcon.run(sql)
        elif not self.force:
            sql = """UPDATE "%s".history SET status = '%s done' WHERE id = %s""" %(db.schema, s, id + 1)
            res = db.pgcon.run(sql)



    def getDataFromWeb(self, type = LISTINGS):
        uniqueString = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(16))
        timestamp = str(int(time.time()))
        hashstr =  sha1((gCallerId + timestamp + gUniqueKey + uniqueString).encode('utf-8')).hexdigest()
        urlBase = 'HTTP://api.booli.se//'

        if type == LISTINGS:
            urlBase += 'listings?'
        elif type == SOLD or type == LASTSOLD:
            urlBase += 'sold?'
        else:
            return  err('Wrong API type %s' %type)

        area_list = ['64', #Skane
                     '160', #Halland
                     '23', # vastra gotalands
                     '2',  # Stockholm lan
                     '783', # Kronobergs lan
                     '45', #Blekinge lan
                     '381', # Kalmar
                     '153', # Jonkoping
                     '253', # ostergotland lan
                     '26', # sodermanlands lan
                     '145', # Blekinge lan
                     ] #
        #area = '64'
        for idx, area in enumerate(area_list):
            logging.info("Donwloading for area %s" %(area))
            url = urlBase + \
                  'areaId=' + area + \
                  '&callerId=' + gCallerId + \
                  '&time=' + timestamp + \
                  '&unique=' + uniqueString + \
                  '&hash=' + str(hashstr)
            #print (url)
            #exit()
            data = urlopen(url).read().decode('utf-8')
            dic = json.loads(data)
            maxcount = int(dic['totalCount'])
            #print (maxcount)

            if type == LASTSOLD:
                maxcount = 300

            offset = 0
            limit = 300

            while 1:
                logging.info("%s out of %s" %(offset / limit + 1 , int(maxcount / limit) + 1))
                uniqueString = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(16))
                timestamp = str(int(time.time()))
                hashstr = sha1((gCallerId + timestamp + gUniqueKey + uniqueString).encode('utf-8')).hexdigest()
                url = urlBase + \
                      'areaId=' + area +\
                      '&offset=' + str(offset) +\
                      '&limit=' + str(limit) +\
                      '&callerId=' + gCallerId +\
                      '&time=' + timestamp +\
                      '&unique=' + uniqueString +\
                      '&hash=' + str(hashstr)
                #print (url)
                data = urlopen(url).read().decode('utf-8')
                dic = json.loads(data)
                #today = datetime.datetime.today()
                sql = """
                    INSERT INTO raw-data (download_date,
                                          type,
                                          countyid,
                                          lisints_data)
                    VALUES
                    (                     now(),%s,%s,%s)
                """ %('listings' if type == LISTINGS else 'sold', area, dic)


                #fname = BASE_FOLDER + '/data/booli '+ str(datetime.datetime(today.year, today.month, today.day, today.hour, today.minute, today.second)).replace(':','_') + ' ' + area
                #f = open(fname, 'w')
                #json.dump(dic, f)

                #f.close()
                offset += limit

                if offset >= maxcount:
                    logging.info("Downloading complete")
                    break
                if self.latest:
                    logging.info("Downloading complete")
                    break

                time.sleep(random.randint(15,30))

            if idx < len(area_list)-1:
                time.sleep(random.randint(15, 30))





if __name__ == "__main__":
    prog = svrea()
    sys.exit(prog.run())