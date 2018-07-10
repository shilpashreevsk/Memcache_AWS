import os, re, time, memcache
from flask import Flask, render_template, request, redirect, session
from random import randint
from datetime import datetime
import sys, csv
import pymysql
from werkzeug.utils import secure_filename
#global file_name

ACCESS_KEY_ID = ''
ACCESS_SECRET_KEY = ''
BUCKET_NAME = 's3'

hostname = ''
username = ''
password = ''
database = ''
myConnection = pymysql.connect( host=hostname, user=username, passwd=password, db=database, autocommit = True, cursorclass=pymysql.cursors.DictCursor, local_infile=True)

print "Database Connected"

application = Flask(__name__)
app = application
app.secret_key = 'pass'

def memcache_connect():#Connecting to the memcache
	# mc = memcache.Client(['url-memcache'], debug = 1)
	mc = memcache.Client(['], debug = 1)
	print "Memcache connected"
	return mc

UPLOAD_FOLDER = '/home/ubuntu/flaskapp/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
query= "select * from all_week limit"
@app.route("/")
def hello():#For displaying the first page
    return render_template("filehandle.html")

@app.route("/csv_upload", methods = ['POST'])
def csv_upload():#For uploading the file
	#global file_name
	csv_file = request.files['file_upload']	
	file_name=csv_file.filename
	session['file_name']=file_name
	print "file recieved"
	filename = secure_filename(csv_file.filename)
	csv_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
	drop_query="drop table IF EXISTS "+ file_name[:-4]
	with myConnection.cursor() as cursor:
		cursor.execute(drop_query)
		myConnection.commit()
	print "dropped"
	column_name="("
	abs_filename=UPLOAD_FOLDER+file_name
	with open(abs_filename, 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			line=row
			break
	for i in line:
		column_name+=i+" VARCHAR(50),"
	query="Create table if not exists " + file_name[:-4]+column_name+" sr_no INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(sr_no));"
	print query
	starttime = time.time()
	with myConnection.cursor() as cursor:
		cursor.execute(query)
		myConnection.commit()
	cursor.close()
	print "successfully created"
	#insert_str = r"LOAD DATA LOCAL INFILE + abs_filename + INTO TABLE + file_name[:-4]+  FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 lines"
   	#newline="\\\n"
	#new_char=newline[1:3]
	#print new_char
	insert_str="""LOAD DATA LOCAL INFILE '"""+abs_filename+ """' INTO TABLE """+ file_name[:-4] +""" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"""
	print (insert_str)
	with myConnection.cursor() as cursor:
		cursor.execute(insert_str)
		myConnection.commit()
	endtime = time.time()
	count_str="SELECT count(*) FROM "+ file_name[:-4]
	with myConnection.cursor() as cursor:
		cursor.execute(count_str)
		result=cursor.fetchall()
	print "successfully loaded"
	totalsqltime = endtime - starttime 
	return render_template("filehandle.html",count=result[0]['count(*)'],rdstime0=totalsqltime)

@app.route('/sqlexecute', methods=['POST'])
def sqlexecute():
    	limit = request.form['limit']
    	starttime = time.time()
    	print(starttime)
    	with myConnection.cursor() as cursor:
        	cursor.execute(query + limit)
	        myConnection.commit()
    	cursor.close()        
    	endtime = time.time()
    	print('endtime')
    	totalsqltime = endtime - starttime
    	print(totalsqltime)
    	return render_template('filehandle.html', rdstime1=totalsqltime)

@app.route('/cleanexecute',methods=['POST'])
def cleanexecute():
	val=request.form['limit']
	with myConnection.cursor() as cur:
    		save="savepoint s1"
	    	cur.execute(save)
	    	print "save point created"
	    	safeupdate="SET SQL_SAFE_UPDATES = 0"
	    	cur.execute(safeupdate)
	    	cleanquery="update Education set STATE='AK' where INSTNM ='"+val+"';"
	    	cur.execute(cleanquery)
	    	print "executed query"
	    	s="select * from Education where INSTNM='"+val+"';"
	    	cur.execute(s)
	    	result = cur.fetchall()
	    	#c = 0
	    	#str1 = " "
	    	#for row in result:
        	#	c = c + 1
        	#	print str(c) + ':' + str(row)
	        #	str1 += str(c) + ':' + str(row) + '<br><br>'
    		myConnection.commit()
		cur.close()
    	return render_template('filehandle.html',tableData1=result)

@app.route('/sqlexecuterestrict', methods=['POST'])
def sqlexecuterestrict():
    	limit = request.form['limit']
	print limit
	locquery="Select * from all_week where locationSource='"+limit+"';"
    	starttime = time.time()
    	print(starttime)
    	with myConnection.cursor() as cursor:
        	cursor.execute(locquery)
	        myConnection.commit()
    	cursor.close()        
    	endtime = time.time()
    	print('endtime')
    	totalsqltime = endtime - starttime
    	print(totalsqltime)
    	return render_template('filehandle.html', rdstime2=totalsqltime)

@app.route('/sqlexecuterestrictlat', methods=['POST'])
def sqlexecuterestrictlat():
    	longitude= request.form['long']
	latitude = request.form['lat']
	locquery="Select * from all_week where latitude="+latitude+" and longitude="+longitude+";"
	print (locquery)
    	starttime = time.time()
    	print(starttime)
    	with myConnection.cursor() as cursor:
        	cursor.execute(locquery)
	        myConnection.commit()
    	cursor.close()        
    	endtime = time.time()
    	print('endtime')
    	totalsqltime = endtime - starttime
    	print(totalsqltime)
    	return render_template('filehandle.html', rdstime3=totalsqltime)

@app.route('/memexecute', methods=['POST'])
def imp_memcache():#For implementing memcache
	mc = memcache_connect()
	#mc.flush_all()
	limit = request.form['limit']
	print limit
	locquery="select * from all_week limit"+limit+";"
	print (locquery)
	new_key = locquery.replace(' ','')
    	value = mc.get(new_key) #value = mc.get("key")
	print value
	starttime = time.time()
    	print(starttime)
    	if value is None:
		print "entered memcache"
		with myConnection.cursor() as cursor:
        		cursor.execute(locquery)
		        rows=cursor.fetchall()
			result=" "
			for i in rows:
				result+=str(i)
			cursor.close()
			print "fetched"
			print (result)
			status = mc.set(new_key,result) 
			print (status)
			print (mc.get(new_key))			
	else:
		print (mc.get(new_key))        
    	endtime = time.time()
    	print('endtime')
    	totalsqltime = endtime - starttime
    	print(totalsqltime)
    	return render_template('filehandle.html', rdstime4=totalsqltime)

@app.route('/query1', methods=['POST'])
def imp_query1():
	state= request.form['val1']
	query1="Select * from Education where State='"+state+"';"
	query2="Select count(*) from Education where State='"+state+"';"
	print(query1)
	print(query2)
	with myConnection.cursor() as cursor:
        		cursor.execute(query1)
		        rows=cursor.fetchall()
			cursor.execute(query2)
			result=cursor.fetchall()
			cursor.close()
	return render_template('filehandle.html',tableData=rows,no=result[0]['count(*)'])

@app.route('/query2', methods=['POST'])
def query2():
    x = request.form['val1']
    y = request.form['val2']
    w = float(x)
    z = float(y)
    temp1 = w - 2
    temp2 = z + 2
    longitude = str(temp1)
    latitude  = str(temp2)
    locquery = "Select distinct(all_month.id) from all_month where longitude between '"+x+"' and '"+longitude+"' and latitude between '" + y + "' and '" +latitude+ "';"
    print("hiii")
    print (locquery)
    starttime = time.time()
    #print(starttime)
    with myConnection.cursor() as cursor:
        cursor.execute(locquery)
        result = cursor.fetchall()
        for el in result:
            print(el['id'])
        myConnection.commit()
        cursor.close()
        endtime = time.time()
        #print('endtime')
        totalsqltime = endtime - starttime
        #print(totalsqltime)
        return render_template('filehandle.html', res=result)
