from os import getenv
from dotenv import load_dotenv
from gevent.queue import Queue, Empty
from gevent import time
from gevent.threadpool import ThreadPool

load_dotenv()

class ConnectionPool:

    """Class that creates a connection pool to manage Database connections. 
    Params:
        dbms : DBMS to connect to. "redshift" and "mysql" available.
        init_conns : The number of DB connections established at the time of initialization
        idle_time : Duration of inactivity after which connection should be closed and flushed out of pool
        db : Database to connect to, applicable for Mysql DBMS """

    def __init__(self, dbms:str='redshift',init_conns:int=5,idle_time:int=600,db:str=None):

        if dbms not in ('redshift','mysql'):
            raise AttributeError('Only "redshift" or "mysql" dbms provided')
        self._dbms = dbms
        if self._dbms=='redshift':
            import redshift_connector as rc
            self._connector=rc        
        else:
            import mysql.connector as mc
            self._connector=mc

        self.syntax_error = self._connector.ProgrammingError
        self.database_error = self._connector.DatabaseError
        self.operational_error = self._connector.OperationalError
        self._db=db
        self._pool = Queue(0)
        self._idle_time = idle_time
        # print('idle time :',idle_time)
        self.__initialize_connections(init_conns)
        self.__threadpool=ThreadPool(1)
        # Background greenlet process to close connections
        self.__threadpool.spawn(self.close_idle_connections)

    def __initialize_connections(self,init_conns):
        """Initialize the connection pool with a certain number of connections."""
        for _ in range(init_conns):
            conn = self.__create_new_connection()
            self._pool.put(conn)
        # print('connections set at : ',time.time())

    def __create_new_connection(self):
        """Create a new Redshift or mysql connection."""
        if self._dbms=='redshift':
            conn = self._connector.connect(
                    host=getenv("REDSHIFT_HOST"),
                    database=getenv("REDSHIFT_DATABASE"),
                    user=getenv("REDSHIFT_USER"),
                    password=getenv("REDSHIFT_PASSWORD"),
                    timeout=int(getenv("REDSHIFT_TIMEOUT")))
        else:
                       
            conn=self._connector.connect(user=getenv("MYSQL_USER"),
                                    password=getenv("MYSQL_PASSWORD"),
                                    database=getenv("MYSQL_DATABASE"),
                                    host=getenv("MYSQL_HOST"),
                                    connection_timeout=int(getenv("MYSQL_TIMEOUT")))
        # result= [conn,time.time()]
        # print('conn :',result)
        return [conn,time.time()]
    
    def close_idle_connections(self):
        """Close connections that have been idle for a certain duration."""
        while True:
            time.sleep(self._idle_time)
            # print('closing process started at : ',time.time())
            put=0      
            closed=0    
            for i in range(0,self._pool.qsize()):  
                print(i)
                try:
                    conn=self._pool.get(block=False)
                    if time.time()-conn[1]>=self._idle_time:
                        try:
                            conn[0].close()
                            closed+=1
                        except:
                            pass
                    else:
                        self._pool.put(conn)
                        put+=1
                except Empty:
                    break
            # print({'response':f"{put} returned to pool and {closed} closed"})      
        # self.__threadpool.spawn(self.close_idle_connections,manager=True)

        # print(f'{len(connections)} returned to pool and {closed} closed')


    def get_connection(self):
        """Get a connection from the pool."""
        try:
            conn = self._pool.get(block=False)
            if time.time()-conn[1]>=self._idle_time:
                try:
                    conn[0].close()
                except:
                    pass
                conn = self.__create_new_connection()
        except Empty:
            # Pool is empty, create a new connection if necessary
            conn = self.__create_new_connection()

        return conn

    def return_connection(self, conn):
        """Return a connection back to the pool."""
        try:
            self._pool.put(conn, block=False)
        except Exception as e:
            pass

    def close_all_connections(self):
        """Close all connections in the pool."""
        while not self._pool.empty():
            conn = self._pool.get()[0]
            try:
                conn.close()
            except Exception as e:
                pass
