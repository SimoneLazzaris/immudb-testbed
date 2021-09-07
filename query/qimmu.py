#!/usr/bin/env python3
import immudb.client, csv, time, logging, random

logging.basicConfig(format="%(asctime)s %(message)s", level=logging.DEBUG)
connection = immudb.client.ImmudbClient()
connection.login("immudb","immudb")
#connection.sqlQuery("select * from geolocation where geonameid={}".format(9963802))
t0=time.time()
found=0
for i in range(1,1000):
    xid=random.randrange(10000000)
    ret=connection.sqlQuery("select * from geolocation where geonameid={}".format(xid))
    found+=len(ret)
    logging.info(ret)
t1=time.time()

logging.info("Elapsed: {} found {}".format(t1-t0,found))
