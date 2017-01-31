#!/usr/bin/env python

import logging
import psycopg2



class pgProcess:


    def close(self):
        self.pgConnection.close()

    def __del__(self):
        self.pgConnection.close()

    def __init__(self, database = 'booli1', host = '127.0.0.1', port = '5432', user = 'pguser', password = None):

        self.db = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password

        self.pgConnection = None

        try:
            self.pgConnection = psycopg2.connect(database = self.db,
                                                  user = self.user,
                                                  password = self.password,
                                                  host = self.host,
                                                  port = self.port)
            self.pgConnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        except Exception as e:
            logging.info('Cannot connect to Postgres\n %s' %e)

    def check_connection(self):
        print(self.pgConnection.status)

    def run(self, sql, isSelect = False):

        try:
            cursor = self.pgConnection.cursor()
            cursor.execute(sql)

        except Exception as e:
            logging.error('Error while executing query\nQuery:\n%s\nError:\n%s' %(sql, e))
            cursor.close()
            return 1

        if isSelect is False:
            cursor.close()
            return 0

        res = cursor.fetchall()
        cursor.close()
        return res

