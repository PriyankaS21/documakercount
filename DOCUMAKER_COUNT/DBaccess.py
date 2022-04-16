import ibm_db
import configparser, logging


# SETTINGS FOR LOGGING
logger = logging.getLogger('DBAccess')    
logger.setLevel(logging.INFO)
formater = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
fileHandler = logging.FileHandler('LOGS\\DocumakerCount.log')
fileHandler.setFormatter(formater)
logger.addHandler(fileHandler)

 
# CONNECT TO IBM_DB DATABASE
def connDB(conlist): 
    try:
        print(f'Connecting IBM DB2: {conlist[0]} - Initiated')
        dbIBMConn = ibm_db.connect(f'DATABASE={conlist[2]};HOSTNAME={conlist[0]};PORT={conlist[1]};PROTOCOL=TCPIP;UID={conlist[3]};PWD={conlist[4]};', "", "")
        
    except Exception as e:
        print(f'ERROR dbConnIBM - CONNECTING IBM DB\n--> Please Check Your VPN Connection <--')        
        logger.error(f'ERROR dbConnIBM CONNECTING IBM DB2 SERVER: {conlist[0]}, DBName: {conlist[2]}, UID={conlist[3]}, PWD={conlist[4]}\n{e}')
    
    else:
        print(f'IBM DB2 Connection to {conlist[0]} - Success')
        logger.info(f'IBM DB2 Connection to {conlist[0]} - Success')
        return dbIBMConn
 

# METHOD TO RUN DB QUERY 
def runQuery(dbIBMConn,query):
    try:
        # dbIBMConn = connDB()
        stmtprp = ibm_db.exec_immediate(dbIBMConn, query)        
        rowData = ibm_db.fetch_assoc(stmtprp)
        #print(rowData)
 
    except Exception as e:
        print(f'ERROR runQueryIBM - {e}')
        logger.error(f'ERROR runQueryIBM - {e}')
        
    else:        
        return  rowData

