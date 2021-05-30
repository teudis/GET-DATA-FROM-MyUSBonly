import sqlite3
import MySQLdb
import socket
import schedule
import time
import platform
import ConfigParser
import os


def read_bd():  
  sistema = platform.system()
  version  = platform.release()
  ruta_xp = ""
  ruta_other = ""
  windows = "windows_xp"
  if sistema=="Windows" and version=="XP":
          ruta_xp = os.environ['ALLUSERSPROFILE']
          ruta_xp = ruta_xp+'\Datos de programa\IceDeviceCon\AccessLog.db'
          
  else:      
          ruta_other = os.environ['PROGRAMDATA']
          ruta_other = ruta_other+'\IceDeviceCon\AccessLog.db'
          windows = "windows_others"
  
  if windows=="windows_xp" and os.path.isfile(ruta_xp) or windows=="windows_others" and  os.path.isfile(ruta_other):
    
    conn = ""
    if windows=="windows_xp" and os.path.isfile(ruta_xp):
      conn = sqlite3.connect(ruta_xp)
      
    else:
       conn = sqlite3.connect(ruta_other)
  
    c = conn.cursor()
    print "Conexion a BD SQLITE"
    c.execute('Select  id,User,"Action",time ,FileName,Extension1,Extension2,DeviceName,FileName_Old,FileName_New from AccessDB_WatchFolder')

    # Fetch a single row using fetchone() method.
    qSQLresults = c.fetchall()
    # abrimos conexion mysql
    # Establecemos la conexión con la base de datos
    password_base = ""
    config = ConfigParser.RawConfigParser()
    config.read('data.cfg')
    host = config.get('base', 'host'.encode('utf8'))
    user = config.get('base', 'user'.encode('utf8'))
    password_base  = config.get('base', 'password_base'.encode('utf8'))
    data = config.get('base', 'data'.encode('utf8'))

    bd = MySQLdb.connect(host,user,password_base,data )
    bd.set_character_set('utf8')
    print "Conexion a BD MYSQL"
    # Preparamos el cursor que nos va a ayudar a realizar las operaciones con la base de datos
    cursor = bd.cursor()
    for row in qSQLresults:
      id = row[0]
      user = row[1]
      action = row[2]
      time = row[3]
      filename = row[4]
      extension1 = row[5]
      extension2 = row[6]
      device_name = row[7]
      filename_old = row[8]
      filename_new = row[9]
      pc = socket.gethostname()     
      cursor.execute("SELECT * FROM watch_usb WHERE id = %s AND user = %s AND pc = %s", (id, user,pc))
      data = cursor.fetchone()
      if data is None:
        #SQL query insert in wacth usb
        cursor.execute('''INSERT into watch_usb (id,user,action,time,filename,extension1,extension2,device_name,file_oldname,file_newname,pc)
                values (%s, %s , %s, %s ,%s, %s, %s, %s, %s ,%s , %s)''',
                (id, user,action,time,filename,extension1,extension2,device_name,filename_old,filename_new,pc))
         # Commit your changes in the database
        bd.commit()
        

    # Search devices inserted
    c.execute('Select id,User,Time,"Action", DeviceName from AccessDB_USBDevice')   
    qSQLresults = c.fetchall() 
    for row in qSQLresults:
      id = row[0]
      user = row[1]
      time = row[2]
      action = row[3]
      devicename = row[4]
      pc = socket.gethostname()
      cursor.execute("SELECT * FROM usb_devices WHERE id = %s AND user = %s AND pc = %s", (id, user,pc))
      data = cursor.fetchone()
      if data is None:
        #SQL query insert in wacth usb
        cursor.execute('''INSERT into usb_devices (id,user,action,time,device_name,pc)
                values (%s, %s , %s, %s ,%s, %s)''',
                (id, user,action,time,devicename,pc))
         # Commit your changes in the database
        bd.commit()
        
    print "Cerrando conexiones"        
    bd.close()

    c.close()
    conn.close()
  else:
    print "cancelada"
    return schedule.CancelJob 
schedule.every(15).minutes.do(read_bd)
#schedule.every(1).hours.do(read_bd)

while True:
    schedule.run_pending()
    time.sleep(45)
