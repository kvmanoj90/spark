
#import sparkContect
from pyspark import SparkContext

def computeContribs(neigbour,ranks):
    #calculate no of neigbours for page
	li=len(neigbour)
    #divide the present ranks by length and get new rank
	div_rank=ranks/li
    #create an empty list
	ar=list()
    #loop neogbour array and create key value pair with key as page and value as new rank
    for x in neigbour:
        ar.append((x,div_rank))
    return ar

sc=SparkContext()
sc.start()

#load a file
links=sc.textFile(file)

#split the line into array  - o/p will be ['n','ig','12']

links1=links.map(lambda line:line.split())

#take lines first field and second field as a set and make sure you have disticnt pair and group by keys -- (k1,[v1,v2,v3]) will be the new format
links2-links1.map(lambda pages: (pages[0],pages[1]).distinct().groupByKey()

#write it to hard disk
links3 =links2.persist()

#create a rdd ranks with (page_no,1) as o/p
ranks=links3.map(lambda(page,neig):(page,1))

#now loop for 10 times or more
for x in range (10):
    #join links with ranks
    #new o/p rdd will be (p1,([p3,p4],1)
    #now make it flatmap using computeContribs  take values,rank of rdd and pass to func & o/p (p3,0.5)(p4,0.5)
	contribs=links.join(ranks).flatmap(lambda(page,(neig,rank)):computeContribs(neig,ranks))
    #reduce that rdd by key and now make some calculations of  value
    ranks=contribs.reduceByKey(lambda v1,v2:v1+v2)).map(lambda (page,contrib):(page,contrib*0.85+0.15))

for rank in ranks.collect():
    print rank

sc.stop()