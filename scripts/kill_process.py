import sys,os

def kill_process_by_name(name):
  cmd = "ps | grep %s" % name
  f = os.popen(cmd)
  txt = f.readlines()
  if len(txt) == 0:
    print "no process \"%s\"!!" % name
    return
  else:
    print txt
    for line in txt:
      if "grep" in line:
        print "skip!!"
        continue

      colum = line.split()
      print colum
      pid = colum[0]


      if 'kill_process.py' in colum[5]:
        continue;
      cmd = "kill -9 %d" % int(pid)
      print cmd


      rc = os.system(cmd)
      if rc == 0 : 
        print "exec \"%s\" success!!" % cmd
      else:
        print "exec \"%s\" failed!!" % cmd
  return

if __name__ == "__main__":
  if len(sys.argv) == 1:
    name=raw_input("plz input the process name which you want to kill :")
  else:
    name=sys.argv[1]
  kill_process_by_name(name)
