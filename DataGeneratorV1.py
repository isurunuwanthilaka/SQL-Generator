##Author : Isuru Nuwanthilaka <isurunuwanthilaka@gmail.com>
##Date   : 2018 Nov 10

import itertools,time,random,psycopg2

#creating databse connection
conn = psycopg2.connect("host=localhost dbname=dev user=postgres password=test")
cur = conn.cursor()

##configurations
limit=10
count=0
arr =['a','1','Z','3','5','g','H','i','j','r','f','P','6','u','X','9','w','b','2','c','D','7','R','n','t','p','4']
codelength=6
bulk_size = 10#number of value tuples need to be included to one sql statement.  

start =time.time()

###functions

def buildinsertquery(arr):
    
    '''This func is for creating sql query for a give array of tokens'''

    ## EDIT this SQL statement
    query_statement = "INSERT INTO public.token (id,reference,token_batch_id,valid_from,valid_to,token_status_id,created_by) VALUES"

    for i in arr:
        add_string = "(DEFAULT,'%s',3,'2018-11-13','2018-11-13',1,1),"%(i)
        query_statement += add_string

    query_statement = query_statement[:-1]+";\n"
    return query_statement


#main func

#open the file
foa = open("mock_data.sql","a+")
fob = open("details.txt","a+")

#creating tokens
print("start token generation.")
fob.write("start token generation.\n")
codeArr = []
for i in itertools.permutations(arr,codelength):
    count+=1
    j="".join(i)
    codeArr.append(j)
    
    if count==limit:
        break

print("finish token generation.")
fob.write("finish token generation.\n")
print("open files to write data.")
fob.write("open files to write data.\n")

#shuffle codeArr
print("shuffle token.")
fob.write("shuffle token.\n")
random.shuffle(codeArr)

print("start writing to files and database")
fob.write("start writing to files and database\n")
fob.close()
for i in range(0,len(codeArr),bulk_size):
    #creating sql query and write to file
    insert_query = buildinsertquery(codeArr[i:i+bulk_size])
    #write to file
    foa.write(insert_query)
    #write to postgres db
    cur.execute(insert_query)
    conn.commit()
    print("bulk %s finished."%((i//bulk_size)+1))
    fob = open("details.txt","a+")
    fob.write("bulk %s finished.\n"%((i//bulk_size)+1))
    fob.close()
    time.sleep(1)


print("finish writing to files and database")
end=time.time()
##writing process details
fob = open("details.txt","a+")
fob.write("finish writing to files and database.\n")
fob.write("Process on %s\nSuccessfully generated %d\nElapsed time %fs\n"%(time.asctime(time.localtime()),len(codeArr),end-start))
fob.write("program exit successfully.\n")

#realeasing resources
foa.close()
fob.close()

print("program exit successfully.")





