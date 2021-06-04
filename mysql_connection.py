#!/usr/bin/python3
# -*- coding: latin-1 -*-
import os
import mysql.connector

home=str(os.path.expanduser("~"))
f=open(os.path.join(os.path.expanduser("~"),".database_credentials"),"r")
  
condata={}

for x in f:
  condata[str(x.split('=')[0]).strip()]=str(x.split('=')[1]).strip()

def run_select_array_ret(select, conn):
  result = []
  cursor = conn.cursor()
  cursor.execute(select)
  result=cursor.fetchall()
  result=[res[0] for res in result]
  return result

def run_select(select,conn):
  result = []
  cursor = conn.cursor(dictionary=True)
  cursor.execute(select)
  result=cursor.fetchall()
  return result


def run_sql(sql,conn):
  if not conn.is_connected():
    conn.reconnect()
    
  cursor = conn.cursor()

  try:
    cursor.execute(sql)
  except mysql.connector.errors.OperationalError:
    conn.reconnect()
    cursor.execute(sql)

  conn.commit()
  return True


def new_conn():
  conn = mysql.connector.connect(
    host=condata['DB_HOST'],
    user=condata['DB_USERNAME'],
    port=condata['DB_PORT'],
    database=condata['DB_DATABASE'],
    password=condata['DB_PASSWORD']
  )
  return conn