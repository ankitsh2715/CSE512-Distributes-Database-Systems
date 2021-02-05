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
            
            createQuery = "CREATE TABLE " + rangePartitionTableName + " (userid integer, movieid integer, rating float);"
            cur.execute(createQuery)
            
            #because for first partition range is [0, interval]
            if(i==0):
                insertQuery = "INSERT INTO " + rangePartitionTableName + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating >= " + str(minRating) + " and rating <= " + str(maxRating) + ";"
                ########print(insertQuery)
            #because second partition onwards range is (interval, interval*2]
            else:
                insertQuery = "INSERT INTO " + rangePartitionTableName + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating > " + str(minRating) + " and rating <= " + str(maxRating) + ";"
                ########print(insertQuery)
            cur.execute(insertQuery)
    
    cur.close()
    openconnection.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    #create partitions
    for i in range(numberofpartitions):
        rrPartitionTableName = RROBIN_TABLE_PREFIX + str(i)
        createQuery = "CREATE TABLE " + RROBIN_TABLE_PREFIX + str(i) + " (userid integer, movieid integer, rating float);"
        cur.execute(createQuery)
    
    #add column which gives rownumber value for ratings table
    alterQuery = "ALTER TABLE " + ratingstablename + " ADD rownumber serial;"
    cur.execute(alterQuery)
    
    #select all rows that will inserted into the fragment when using rrobin technique
    for i in range(numberofpartitions):
        insertQuery = "INSERT INTO " + RROBIN_TABLE_PREFIX + str(i) + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE (rownumber-1)%" + str(numberofpartitions) + "=" + str(i) +";"
        #########print(insertQuery)
        cur.execute(insertQuery)
    
    #drop extra column that we created
    alterQuery = "ALTER TABLE " + ratingstablename + " DROP COLUMN rownumber;"
    cur.execute(alterQuery)
    
    cur.close()
    openconnection.commit()
    
def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    #count number of rows in ratings table
    countQuery = "SELECT COUNT(*) FROM " + ratingstablename +";"
    cur.execute(countQuery)
    count = cur.fetchone()
    totalRows = count[0]
    #count number of rrobin partitions we must have created
    countPartitionsQuery = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{RROBIN_TABLE_PREFIX}%'"
    cur.execute(countPartitionsQuery)
    count = cur.fetchone()
    totalPartitions = count[0]
    #calculate index of partition where we need to insert new tuple
    lastInsertTableIndex = totalRows%totalPartitions
    #insert tuple in ratings table
    insertQueryRatings = "INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    cur.execute(insertQueryRatings)
    #insert tuple in rrobin partition table
    insertQueryRRobinPartition = "INSERT INTO " + RROBIN_TABLE_PREFIX + str(lastInsertTableIndex) + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    cur.execute(insertQueryRRobinPartition)
    
    cur.close()
    openconnection.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    
    #count number of range partitions we must have created
    countPartitionsQuery = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{RANGE_TABLE_PREFIX}%'"
    cur.execute(countPartitionsQuery)
    count = cur.fetchone()
    totalPartitions = count[0]
    
    #to calculate range for ratings
    interval = 5.0/totalPartitions
    
    for i in range(totalPartitions):
        inThisPartition = False
        #getting range for partitions in similar way I calculated the range in rangePartition()
        rangePartitionTableName = RANGE_TABLE_PREFIX + str(i)
        minRating = i * interval
        maxRating = minRating + interval
        # for i=0 [minRating, maxRating]
        if i==0:
            if rating>=minRating and rating<=maxRating:
                inThisPartition = True
        else: #for i>0 (minRating, maxRating]
            if rating>minRating and rating<=maxRating:
                inThisPartition = True
        
        if inThisPartition: # if rating input falls in this partition then add it
            insertQueryRRobinPartition = "INSERT INTO " + RANGE_TABLE_PREFIX + str(i) + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
            cur.execute(insertQueryRRobinPartition)
            insertQueryRatings = "INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
            cur.execute(insertQueryRatings)
            break
    
    cur.close()
    openconnection.commit()


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
