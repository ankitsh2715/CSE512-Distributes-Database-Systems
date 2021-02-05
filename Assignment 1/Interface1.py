RANGE_TABLE_PREFIX = 'range_ratings_part'
RROBIN_TABLE_PREFIX = 'round_robin_ratings_part'

import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection
    cur = conn.cursor()
    
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
    conn.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    
    if(numberofpartitions > 0):
        #to calculate range for ratings
        interval = 5.0 / numberofpartitions
    
        for i in range(numberofpartitions):
            rangePartitionTableName = RANGE_TABLE_PREFIX + str(i)
            minRating = i * interval
            maxRating = minRating + interval
            
            createQuery = "CREATE TABLE " + rangePartitionTableName + " (userid integer, movieid integer, rating float);"
            cur.execute(createQuery)
            
            if(i==0): #because for first partition range is [0, interval]
                insertQuery = "INSERT INTO " + rangePartitionTableName + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating >= " + str(minRating) + " and rating <= " + str(maxRating) + ";"
            else: #because second partition onwards range is (interval, interval*2]
                insertQuery = "INSERT INTO " + rangePartitionTableName + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating > " + str(minRating) + " and rating <= " + str(maxRating) + ";"
            cur.execute(insertQuery)
    
    cur.close()
    conn.commit()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    
    #create partitions
    for i in range(numberofpartitions):
        rrPartitionTableName = RROBIN_TABLE_PREFIX + str(i)
        createQuery = "CREATE TABLE " + RROBIN_TABLE_PREFIX + str(i) + " (userid integer, movieid integer, rating float);"
        cur.execute(createQuery)
    
    #add column to get rownumber value for ratings table
    alterQuery = "ALTER TABLE " + ratingstablename + " ADD rownumber serial;"
    cur.execute(alterQuery)
    
    #select all rows that should be inserted into the corresponding fragment when using rrobin technique
    for i in range(numberofpartitions):
        insertQuery = "INSERT INTO " + RROBIN_TABLE_PREFIX + str(i) + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE (rownumber-1)%" + str(numberofpartitions) + "=" + str(i) +";"
        cur.execute(insertQuery)
    
    #drop extra column that we created
    alterQuery = "ALTER TABLE " + ratingstablename + " DROP COLUMN rownumber;"
    cur.execute(alterQuery)
    
    cur.close()
    conn.commit()
    
def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    
    #count number of rows in ratings table
    countQuery = "SELECT COUNT(*) FROM " + ratingstablename +";"
    cur.execute(countQuery)
    count = cur.fetchone()
    totalRows = count[0]
    
    #function call to get total rrobin partitions created
    totalPartitions = countPartitions(RROBIN_TABLE_PREFIX, cur)
    
    #calculate index of partition where we need to insert new tuple
    lastInsertTableIndex = totalRows % totalPartitions
    
    #insert tuple in ratings table
    insertQueryRatings = "INSERT INTO " + ratingstablename + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    cur.execute(insertQueryRatings)
    
    #insert tuple in rrobin partition table
    insertQueryRRobinPartition = "INSERT INTO " + RROBIN_TABLE_PREFIX + str(lastInsertTableIndex) + "(userid, movieid, rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");"
    cur.execute(insertQueryRRobinPartition)
    
    cur.close()
    conn.commit()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    
    totalPartitions = countPartitions(RANGE_TABLE_PREFIX, cur)
    
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
    conn.commit()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #remove file if created because of earlier function calls from test cases
    if os.path.exists(outputPath):
        os.remove(outputPath)
    
    conn = openconnection
    cur = conn.cursor()
    
    #function call to get total range partitions created
    totalRangePartitions = countPartitions(RANGE_TABLE_PREFIX, cur)
    #write tuples to output file looping through all range partitions
    for i in range(totalRangePartitions):
        
        selectQuery = "SELECT * FROM " + RANGE_TABLE_PREFIX + str(i) + " WHERE rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";"
        cur.execute(selectQuery)
        row = cur.fetchone()
        while row:
            res = f"{RANGE_TABLE_PREFIX}{i},{row[0]},{row[1]},{row[2]}"
            saveToFile(res, outputPath)
            row = cur.fetchone()
    
    #function call to get total rrobin partitions created
    totalRRobinPartitions = countPartitions(RROBIN_TABLE_PREFIX, cur)
    #write tuples to output file looping through all rrobin partitions
    for i in range(totalRRobinPartitions):
        
        selectQuery = "SELECT * FROM " + RROBIN_TABLE_PREFIX + str(i) + " WHERE rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue) + ";"
        cur.execute(selectQuery)
        row = cur.fetchone()
        while row:
            res = f"{RROBIN_TABLE_PREFIX}{i},{row[0]},{row[1]},{row[2]}"
            saveToFile(res, outputPath)
            row = cur.fetchone()
    
    cur.close()
    conn.commit()


def pointQuery(ratingValue, openconnection, outputPath):
    #remove file if created because of earlier function calls from test cases
    if os.path.exists(outputPath):
        os.remove(outputPath)
    
    conn = openconnection
    cur = conn.cursor()
    
    #function call to get total range partitions created
    totalRangePartitions = countPartitions(RANGE_TABLE_PREFIX, cur)
    #write tuples to output file looping through all range partitions
    for i in range(totalRangePartitions):
        selectQuery = "SELECT * FROM " + RANGE_TABLE_PREFIX + str(i) + " WHERE rating = " + str(ratingValue) + ";"
        cur.execute(selectQuery)
        row = cur.fetchone()
        while row:
            res = f"{RANGE_TABLE_PREFIX}{i},{row[0]},{row[1]},{row[2]}"
            saveToFile(res, outputPath)
            row = cur.fetchone()
    
    #function call to get total rrobin partitions created
    totalRRobinPartitions = countPartitions(RROBIN_TABLE_PREFIX, cur)
    #write tuples to output file looping through all rrobin partitions
    for i in range(totalRRobinPartitions):
        selectQuery = "SELECT * FROM " + RROBIN_TABLE_PREFIX + str(i) + " WHERE rating = " + str(ratingValue) + ";"
        cur.execute(selectQuery)
        row = cur.fetchone()
        while row:
            res = f"{RROBIN_TABLE_PREFIX}{i},{row[0]},{row[1]},{row[2]}"
            saveToFile(res, outputPath)
            row = cur.fetchone()
    
    cur.close()
    conn.commit()


def countPartitions(tablePrefix, cur):
    countPartitionsQuery = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' and table_name like '{tablePrefix}%'"
    cur.execute(countPartitionsQuery)
    count = cur.fetchone()
    totalPartitions = count[0]
    return totalPartitions

def saveToFile(resStr, filePath):
    file = open(filePath, 'a')
    file.write(resStr+"\n")
    file.close()

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
