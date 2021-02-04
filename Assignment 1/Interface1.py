RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'

import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    #create ratings table #DelimiterX columns are for extra ':' in '::' as copy_from function only takes single-byte sep input
    createQuery = "CREATE TABLE " + ratingstablename+ "(userid integer, delimiter1 char, movieid integer, delimiter2 char, rating float, delimiter3 char, timestamp bigint);"
    cur.execute(createQuery)
    #copy input file data to ratings table
    ratingsFile = open(ratingsfilepath,'r')
    cur.copy_from(ratingsFile, ratingstablename, sep=':')
    #delete extra columns due to separator not being single-byte and Timestamp column
    alterQuery = "ALTER TABLE " + ratingstablename+ " DROP COLUMN delimiter1, DROP COLUMN delimiter2, DROP COLUMN delimiter3, DROP COLUMN timestamp;" 
    cur.execute(alterQuery)
    #release objects
    cur.close()
    openconnection.commit()


def mytesterfunc():
    
    pass

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    if(numberofpartitions > 0):
        #to calculate range for ratings
        interval = 5.0/numberofpartitions
    
        for i in range(numberofpartitions):
            rangePartitionTableName = RANGE_TABLE_PREFIX + str(i)
            minRating = i * interval
            maxRating = minRating + interval
            #this if condition is not necessary
            if(maxRating > 5):
                maxRating = 5
            
            createQuery = "CREATE TABLE " + rangePartitionTableName + " (userid integer, movieid integer, rating float);"
            cur.execute(createQuery)
            
            #because for first partition range is [0, interval]
            if(i==0):
                insertQuery = "INSERT INTO " + rangePartitionTableName + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating >= " + str(minRating) + " and rating <= " + str(maxRating) + ";"
            #because second partition onwards range is (interval, interval*2]
            else:
                insertQuery = "INSERT INTO " + rangePartitionTableName + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating > " + str(minRating) + " and rating <= " + str(maxRating) + ";"
            cur.execute(insertQuery)
    
    cur.close()
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    insertCur = openconnection.cursor()

    for i in range(numberofpartitions):
        rrPartitionTableName = RROBIN_TABLE_PREFIX + str(i)
        createQuery = "CREATE TABLE " + RROBIN_TABLE_PREFIX + str(i) + " (userid integer, movieid integer, rating float);"
        cur.execute(createQuery)
    
    selectQuery = "SELECT * FROM " + ratingstablename +";"
    cur.execute(selectQuery)
    
    i = 0
    row = cur.fetchone()
    while row:
        index = i % numberofpartitions
        tableName = RROBIN_TABLE_PREFIX + str(index)
        insertQuery = f"INSERT INTO {tableName} (userid, movieid, rating) VALUES ({str(row[0])}, {str(row[1])}, {str(row[2])});"
        print(insertQuery)
        i = i + 1
        row = cur.fetchone()
        insertCur.execute(insertQuery)

    insertCur.close()
    cur.close()
    openconnection.commit()
    
def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass # Remove this once you are done with implementation


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass # Remove this once you are done with implementation


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    pass #Remove this once you are done with implementation


def pointQuery(ratingValue, openconnection, outputPath):
    pass # Remove this once you are done with implementation


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
