from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from datetime import datetime
import time
import subprocess

KEYSPACE = "keyspace1"
b = 100


def main(SIZE):
	cluster = Cluster()
	session = cluster.connect()

	print("creating keyspace....")


	session.execute("""
			CREATE KEYSPACE IF NOT EXISTS %s 
			WITH replication = { 'class': 'SimpleStrategy', 'replication_factor':'2'}
			""" % KEYSPACE)

	session.set_keyspace(KEYSPACE)


	session.execute("""
			CREATE TABLE IF NOT EXISTS table1 (
							id int, 
							value1 text,
							value2 text,
							PRIMARY KEY(id))
			""")


	query = SimpleStatement(""" 
			INSERT INTO table1 (id, value1, value2)
			VALUES (%(id)s, %(value1)s, %(value2)s);""", consistency_level= ConsistencyLevel.TWO)



	for i in range(SIZE):
	   #  session.execute("""
				# INSERT INTO table1 (id, value1, value2) 
				# VALUES ({}, 'A','B'); """.format(i))
		session.execute(query, dict(id=i, value1='A', value2='B'))


	subprocess.call("docker stop cassandra-node2".split())


	r1 = session.execute("""SELECT * from table1 where id <= {} ALLOW FILTERING;""".format(SIZE))
	# r1 = r1.result()


	start = datetime.now()

        for i in range(11):
                session.execute("""
                       UPDATE table1
			SET value1 = 'C', value2 = 'D'
			WHERE id={};""".format(i))


	r2 = session.execute("""SELECT * from table1 where id <= {} ALLOW FILTERING;""".format(10))
	#r2 = r2.result()


	subprocess.call("docker start cassandra-node2".split())

	for i in range(11):

		print(r1[i]," | ", r2[i].value1, r2[i].value2)
		if r2[i].value1 != u'C' or r2[i].value2 !=  u'D':
			print(r2[i].value1, r2[i].value2)
	
	print("end search...")


	end = datetime.now()
	print('Time: {}'.format(end - start))

	time.sleep(30)
	session.execute("DROP KEYSPACE "+ KEYSPACE)

	

if __name__ == "__main__":

	SIZE = 10
	for i in range(b):
		print(i,"-----------------------------------")		
		main(SIZE)
		SIZE  = SIZE * 2
