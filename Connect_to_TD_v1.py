# ### Import packages

# In[1]:


import jaydebeapi as db1
import pandas as pd
import numpy
print (numpy.__version__)
import os



# ### Setting up the environment variables - CLASSPATH and JAVA_HOME

# In[3]:


classpath = '/mnt/jdbcfiles/tdgssconfig-16.20.jar:/mnt/jdbcfiles/terajdbc4-16.20.jar'
os.environ['CLASSPATH'] = classpath

javapath = '/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.151.-1.b12.el6_9.x86_64/'
os.environ['JAVA_HOME'] = javapath


# ### Function to connect to teradata

# In[4]:

class td_connect():
    """
    td_connect is a class that has functions :__init__(self) for initialisation and connect_teradata() for teradata connection 
    
    """ 
    def __init__(self):
        self.jclassname='com.teradata.jdbc.TeraDriver'
        self.jarfile1 = '/mnt/jdbcfiles/terajdbc4-16.20.jar'
        self.jarfile2 = '/mnt/jdbcfiles/tdgssconfig-16.20.jar'
    
    def connect_teradata(self,username, passwd, query, database):
        query = query.lower()
        dbname =  query.split("from")[1]
        dbname = dbname.split(".")[0]
        conn = db1.connect(jclassname=self.jclassname,
                           url='jdbc:teradata://{0}.wal-mart.com/database={1},tmode=ANSI,charset=UTF8,LOGMECH=LDAP'.format(database,dbname),
                            driver_args={'user':username,'password':passwd},
                                      jars = [self.jarfile1,
                                              self.jarfile2])
        try:
            data = pd.read_sql(query, conn)
            return data        
        except:
            print ("\n----Table does not exist/ Bad Credentials-----\n")
