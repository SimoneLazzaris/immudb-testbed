#!/usr/bin/python3
import immudb.client, csv, time, logging
import multiprocessing, queue

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.DEBUG)
def nint(v):
	if v=='':
		return 0
	return abs(int(v))
def nfloat(v):
	if v=='':
		return 0.0
	return float(v)
def init_():
    connection = immudb.client.ImmudbClient()
    connection.login("immudb","immudb")
    data=[]
    connection.sqlExec("""create table geolocation 
      (geonameid integer,
       name varchar,
       asciiname varchar,
       alternatenames varchar,
       latitude integer,
       longitude integer,
       feature_class varchar,
       feature_code varchar,
       country_code varchar,
       cc2 varchar,
       admin1_code varchar,
       admin2_code varchar,
       admin3_code varchar,
       admin4_code varchar,
       population integer,
       elevation integer,
       dem integer,
       timezone varchar,
       modification_date varchar,
       primary key geonameid)
       """) 
    connection.sqlExec("CREATE INDEX ON geolocation(latitude)")

    connection.shutdown()

t0=time.time()
lnum=multiprocessing.Value('i')
ti=t0
step=500
ntask=10

def notify(taskid, rowid):
        with lnum.get_lock():
                lnum.value+=step
        logging.info("Task %d, Total Inserted %d, @ID %d, %f row/sec", taskid, lnum.value, rowid, float(lnum.value)/(time.time()-t0))

def reader(q):
        logging.info("Reader start")    
        with open("allCountries.txt","rt") as f:
                reader=csv.reader(f, delimiter='\t')
                for r in reader:
                        if len(r)!=19:
                                print("Skipping",r[0])
                                continue
                        if len(r[2].encode('utf8'))>200:
                                print("Skipping",r[0])
                                continue
                        if len(r[11].encode('utf8'))>20:
                                print("Skipping",r[0])
                                continue
                        r=[x.replace("'", "_") for x in r]
                        r[0]=nint(r[0])
                        r[3]=r[3].replace("'", "_")
                        r[4]=abs(int(nfloat(r[4])))
                        r[5]=abs(int(nfloat(r[5])))
                        r[14]=nint(r[14])
                        r[15]=nint(r[15])
                        r[16]=nint(r[16])
                        q.put(r)
        logging.info("Reader end")    

def writer(wid, q):
        idb= immudb.client.ImmudbClient()
        idb.login("immudb","immudb")
        nn = 0
        datarow="(%s, '%s', '%s', '%s', %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, '%s', '%s')"
        data=[]
        r=q.get()
        while r!=None:
                data.append(datarow%tuple(r))
                rid=r[0]
                r=q.get()
                nn+=1
                if nn%step==0 or r==None:
                        idb.sqlExec("upsert into geolocation(geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date) values "+",".join(data))
                        data=[]
                        notify(wid, rid)
        logging.info("Writer %d end", wid)    


if __name__=='__main__':                        
    qq = multiprocessing.SimpleQueue()
    rr=multiprocessing.Process(target=reader, args=(qq,))
    rr.start()
    pp=[]
    for i in range(0,ntask):
            pp.append(multiprocessing.Process(target=writer, args=(i,qq)))
            pp[i].start()
    t0=time.time()
    rr.join()
    logging.info("Reader out")
    for i in range(0,ntask):
            qq.put(None)
    for i in range(0,ntask):
            pp[i].join()
#-- oracle
#create table geolocation(
   #geonameid integer primary key,
   #name varchar(200),
   #asciiname varchar(200),
   #alternatenames clob,
   #latitude float,
   #longitude float,
   #feature_class char(1),
   #feature_code varchar(10),
   #country_code char(2),
   #cc2 varchar(200),
   #admin1_code varchar(20),
   #admin2_code varchar(20),
   #admin3_code varchar(20),
   #admin4_code varchar(20),
   #population integer,
   #elevation integer,
   #dem integer,
   #timezone varchar(40),
   #modification_date date);
   
   

 #Name                                      Null?    Type
 #----------------------------------------- -------- ----------------------------
 #GEONAMEID                                 NOT NULL NUMBER(38)             0
 #NAME                                               VARCHAR2(200)          1
 #ASCIINAME                                          VARCHAR2(200)          2
 #ALTERNATENAMES                                     BLOB                   3
 #LATITUDE                                           FLOAT(126)             4
 #LONGITUDE                                          FLOAT(126)             5
 #FEATURE_CLASS                                      CHAR(1)                6
 #FEATURE_CODE                                       VARCHAR2(10)           7
 #COUNTRY_CODE                                       CHAR(2)                8
 #CC2                                                VARCHAR2(200)          9
 #ADMIN1_CODE                                        VARCHAR2(20)           10
 #ADMIN2_CODE                                        VARCHAR2(20)           11
 #ADMIN3_CODE                                        VARCHAR2(20)           12
 #ADMIN4_CODE                                        VARCHAR2(20)           13
 #POPULATION                                         NUMBER(38)             14
 #ELEVATION                                          NUMBER(38)             15
 #DEM                                                NUMBER(38)             16
 #TIMEZONE                                           VARCHAR2(40)           17
 #MODIFICATION_DATE                                  DATE                   18

# INSERT (plain table)
# 2021-08-27 13:08:14,634 Inserted 12132536, 936.800092 seconds, 12951.040431 rows per second

# NOTE: missing
# - some "insertmany" which can leverage multiple values
# - safe args suubstitution
