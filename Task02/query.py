from cassandra.cluster import Cluster
cluster = Cluster()
session = cluster.connect()
session.execute("use cloudcomputingcapstone\n")
import datetime

def query2_1():
    origin = input("Enter origin airport\n")
    print("Fetching query results")
    query = "select * from output2_1 where origin ='"+str(origin)+"'"
    #print(query)
    rows = session.execute(query)
    data = list()
    for row in rows:
        thisrow=list()
        thisrow.append(row.origin)
        thisrow.append(row.carrier)
        thisrow.append(float(row.value))
        data.append(thisrow)
    data.sort(key = lambda x: x[2]) 
    print("----------------------------")
    for row in data[:10]:
        print(row)
    print("----------------------------")



def query2_2():
    origin = input("Enter origin airport\n")
    print("Fetching query results")
    query = "select * from output2_2 where origin ='"+str(origin)+"'"
    rows = session.execute(query)
    data = list()
    for row in rows:
        thisrow=list()
        thisrow.append(row.origin)
        thisrow.append(row.destination)
        thisrow.append(float(row.value))
        data.append(thisrow)
    data.sort(key = lambda x: x[2])
    print("----------------------------")  
    for row in data[:10]:
        print(row) 
    print("----------------------------") 
    
#key,carrier,value
def query2_3():
    origin = input("Enter origin airport\n")
    destination = input("Enter destination airport\n")
    print("Fetching query results")
    query = "select * from output2_3 where key ='"+str(origin)+","+str(destination)+"'"
    rows = session.execute(query)
    #print(query)
    data = list()
    for row in rows:
        thisrow=list()
        thisrow.append(row.key)
        thisrow.append(row.carrier)
        thisrow.append(float(row.value))
        data.append(thisrow)
    data.sort(key = lambda x: x[2])
    print("----------------------------")
    for row in data[:10]:
        print(row)
    print("----------------------------")

def query2_4():
    origin = input("Enter origin airport\n")
    destination = input("Enter destination airport\n")
    print("Fetching query results")
    query = "select * from output2_4 where key ='"+str(origin)+","+str(destination)+"'"
    rows = session.execute(query)
    data = list()
    print("----------------------------")
    for row in rows:
        print(row)
    print("----------------------------")
    



def query3_2():
    origin = input("Enter origin airport\n")
    stopover = input("Enter stopover airport\n")
    destination = input("Enter destination airport\n")
    date1 = input("Enter date of journey\n")
    dt = datetime.datetime.strptime(str(date1),"%Y-%m-%d")
    date2 = str((dt+datetime.timedelta(days=2)).date())
    key1=str(origin)+","+str(stopover)+","+str(date1)
    key2=str(origin)+","+str(destination)+","+str(date2)
    print("----------------------------")
    query = "select * from output3_2_FirstLeg where key ='"+str(key1)+"'"
    print(query)
    rows = session.execute(query)
    print("Here are details of first leg")
    for row in rows:
        print(row)
    query = "select * from output3_2_SecondLeg where key ='"+str(key2)+"'"
    print(query)
    rows = session.execute(query)
    print("Here are details of second leg")
    for row in rows:
        print(row)
    print("----------------------------")

    
    
    

    

    pass


while(True):
    queryinput = input("Enter the query you want to answer\n")
    if queryinput=="2.1":
        query2_1()
    elif queryinput=="2.2":
        query2_2()
    elif queryinput=="2.3":
        query2_3()
    elif queryinput=="2.4":
        query2_4()
    elif queryinput=="3.2":
        query3_2()

