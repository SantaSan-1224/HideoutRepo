# -*- coding: utf-8 -*-
import json, urllib.request, urllib.error
import sys
import syslog
import csv
import re

###Initial value###
args = sys.argv
script = args[0]
all = 0
zbxsv = "http://127.0.0.1/zabbix/api_jsonrpc.php"
headers = {"Content-Type":"application/json-rpc"}
zbx_usr = "Admin"
zbx_upw = "tjnzbx_uc0"
actionlist = []
outpath = None
before_outpath = '/usr/local/zabbix/action/data/before_all_action_status.csv'
after_outpath = '/usr/local/zabbix/action/data/after_all_action_status.csv'
zbx_auth = []
list = []
patternlist = []

###Function###
##function log
def log(script,message,code):
  syslog.openlog(script,syslog.LOG_PID|syslog.LOG_PERROR,syslog.LOG_SYSLOG)
  syslog.syslog(syslog.LOG_ALERT,message)
  syslog.closelog()
  return code

##Zabbix API (Zabbix Authentication)
def zabbix_auth(zbxsv,headers,zbx_usr,zbx_upw):
  try:
#   auth_post = json.dumps({'jsonrpc':'2.0', 'method':'user.login', 'params':{'user':zbx_usr, 'password':zbx_upw}, 'auth':None, 'id': 1})
    auth_post = json.dumps({'jsonrpc':'2.0', 'method':'user.login', 'params':{'user':zbx_usr, 'password':zbx_upw}, 'auth':None, 'id': 1}).encode()
#   request = urllib.Request(zbxsv, auth_post, headers)
    request = urllib.request.Request(zbxsv, auth_post, headers)
    contents = urllib.request.urlopen(request)
    contents_str = contents.read()
    contents_dict = json.loads(contents_str)
    zbx_auth = contents_dict["result"]
  except:
    message = u"error: authentication failed. errcode:101"
    result = log(script,message,101)
    sys.exit(101)
  return zbx_auth

##Zabbix API (Get all actionstatus,actionname.Output CSV.)
def zabbix_action_status_csv(zbxsv,headers,zbx_usr,zbx_upw,outpath):
  try:
    auth_post = json.dumps({
        "jsonrpc": "2.0",
        "method": "action.get",
        "params": {
            "output": ["name","status"]
        },
        "auth": zbx_auth,
        "id": 1
#   })
    }).encode()
#   request = urllib.Request(zbxsv, auth_post, headers)
    request = urllib.request.Request(zbxsv, auth_post, headers)
    contents = urllib.request.urlopen(request)
    contents_str = contents.read()
    contents_dict = json.loads(contents_str)
    allactionlist = contents_dict["result"]

    with open(outpath, mode='w') as f:
      for i in allactionlist:
        f.write(i['status'])
        f.write(',')
#       f.write(i['name'].encode('utf-8'))
        f.write(i['name'])
        f.write('\n')

  except:
     message = u"error: Failed to output the CSV file. errcode:102"
     result = log(script,message,102)
     sys.exit(102)
  return allactionlist

##Zabbix API (Get actionid)
def zabbix_get_actionid(zbxsv,headers,zbx_usr,zbx_upw,actionlist):
  try:
    if all == 0:
      auth_post = json.dumps({
        "jsonrpc": "2.0",
        "method": "action.get",
        "params": {
            "output": ["actionid"],
            "filter": {
                "name": 
                    actionlist
            }
        },
        "auth": zbx_auth,
        "id": 1
#     })
      }).encode()
    else:
      auth_post = json.dumps({
        "jsonrpc": "2.0",
        "method": "action.get",
        "params": {
            "output": ["actionid"]
         },
         "auth": zbx_auth,
         "id": 1
#     })
      }).encode()
#   request = urllib.Request(zbxsv, auth_post, headers)
    request = urllib.request.Request(zbxsv, auth_post, headers)
    contents = urllib.request.urlopen(request)
    contents_str = contents.read()
    contents_dict = json.loads(contents_str)
    actionid_list = contents_dict["result"]
    for i in actionid_list:
      id = i["actionid"]
      list.append(id)
  except:
    message = u"error: Failed to get actionid. errcode:103"
    result = log(script,message,103)
    sys.exit(103)
  return list

##Zabbix API (Update action status)
def zabbix_update_action_status(zbxsv,headers,zbx_usr,zbx_upw,list,actionstat):
  try:
    for actionid in list:
      auth_post = json.dumps({
        "jsonrpc": "2.0",
        "method": "action.update",
        "params": {
            "actionid": actionid,
            "status": actionstat
        },
        "auth": zbx_auth,
        "id": 0
#     })
      }).encode()
#     request = urllib.Request(zbxsv, auth_post, headers)
      request = urllib.request.Request(zbxsv, auth_post, headers)
      contents = urllib.request.urlopen(request)
      contents_str = contents.read()
      contents_dict = json.loads(contents_str)
  except:
     message = u"error: Failed to update the action status. errcode:104"
     result = log(script,message,104)
     sys.exit(104)

###Main###
if __name__ == '__main__':
  ##args
  if args[1] == u'enable':
    actionstat = 0
  elif args[1] == u'disable':
    actionstat = 1
  else:
    message = u"error: '"+args[1]+"' is invalid argument. errcode:105"
    result = log(script,message,105)
    sys.exit(105)

  if args[2] == u'all':
    all = 1
  else:
    actionlistpath = args[2]
    try:
      f = open(actionlistpath)
      lines = f.readlines()
      f.close()
      for line in lines:
        text = line.replace('\n','')
        text = text.replace('\r','')
#       patternlist.append(text.decode("utf-8"))
        patternlist.append(text)
    except:
      message = u"error: No such file or directory: '"+actionlistpath+"' errcode:106"
      result = log(script,message,106)
      sys.exit(106)

  zbx_auth = zabbix_auth(zbxsv,headers,zbx_usr,zbx_upw)
  allactionlist = zabbix_action_status_csv(zbxsv,headers,zbx_usr,zbx_upw,before_outpath)

##Regexp (Pattern Match)
  for pattern in patternlist:
    repatter = re.compile(pattern)
    for line in allactionlist:
      matchList = repatter.findall(line['name'])
      for i in matchList:
#       actionlist.append(i.encode('utf-8'))
        actionlist.append(i)
  if all == 0 and len(actionlist) == 0:
    pass
  else:
    list  = zabbix_get_actionid(zbxsv,headers,zbx_usr,zbx_upw,actionlist)
    zabbix_update_action_status(zbxsv,headers,zbx_usr,zbx_upw,list,actionstat)
    zabbix_action_status_csv(zbxsv,headers,zbx_usr,zbx_upw,after_outpath)
  sys.exit(0)
