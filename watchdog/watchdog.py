import sys
import os
import subprocess
import time
import getpass

#home_dir = '/Volumes/My Passport/Inspira/Dados/Crawlers'
root_path = ''
run_path = ''
crawlers_path = ''
user_id = ''
running_procs = {}

def start_all_crawlers():
	global user_id
	global root_path
	global run_path
	global crawlers_path
	global running_pids
	global running_procs
	
	print("%s" %(crawlers_path))
	
	crawler_dirs = os.listdir(crawlers_path)
	for cd in crawler_dirs:
		exec_path = "%s/%s/%s_crawler.py" %(crawlers_path, cd, cd)
		our_cwd = "%s/%s" %(crawlers_path, cd)
		process = subprocess.Popen([sys.executable, exec_path], cwd=our_cwd)
		running_procs[process.pid] = process
		#print("%s %s/%s/%s.py" %(sys.executable, crawlers_path, cd, cd))
		print("Started process %d" %(int(process.pid)))
		#running_pids[process.pid] = cd
		
def start_crawler(exec_path):
	global user_id
	global root_path
	global run_path
	global crawlers_path
	global running_pids
	
	print("%s" %(crawlers_path))
	#exec_path = "%s/%s/%s.py" %(crawlers_path, cd, cd)
	our_cwd = '/'.join(exec_path.split('/')[:-1])
	process = subprocess.Popen([sys.executable, exec_path], cwd=our_cwd)
	running_procs[process.pid] = process
	print("Started process %d" %(process.pid))
	#running_pids[process.pid] = cd
	
def main():
	global user_id
	global root_path
	global home_dir
	global run_path
	global crawlers_path
	global running_pids
	
	root_path = os.getcwd()
	run_path = root_path + '/run'
	crawlers_path = root_path + '/crawlers'
	
	original_crawler_dirs = os.listdir(crawlers_path)
	
	for cd in original_crawler_dirs:
		if os.path.isdir(cd):
			if os.path.isfile('%s/%s.pid' %(run_path, cd)):
				print("[-] Error! You can't start the watchdog with running PIDs!\n[-] Kill all running crawlers and cleanup the PID files first!\n")
				
				
	start_all_crawlers()
	
	#sys.exit()
	
	while True:
		
		crawler_dirs = os.listdir(crawlers_path)
		"""
		# In future this will allow us to add new crawlers to a running box without having to stop/restart the other running crawlers
		
		for cd in crawler_dirs:
			if os.path.isdir(cd):
				if not os.path.isfile('%s/%s.pid' %(run_path, cd)):
					if cd in original_crawler_dirs:
						print("[-] Crawler %s has gracefully deleted its own pid file. We will restart it now." %(cd))
					else:
						print("[+] We've added a new crawler... starting it up for the first time")
						original_crawler_dirs.append(cd)
					
					start_crawler(cd)
		"""
				
		keys = running_procs.keys()
		kill_pids = []
		restarts = []
		
		for rp in keys:
			print(rp)
			if running_procs[rp].poll() != None:
				print("A process died, Gonna restart a mfqr")
				#
				# hilariously we can't actually finally kill or start a new process inside a loop
				# because python throws a hissy fit that the size of the list it's iterating over
				# has changed.
				# And because python does lazy copies (ref) unless you force it, it's easier to
				# simply build lists of processes to kill/restart once we've iterated. At least
				# the subprocess module makes that fairly painless as a process never really
				# dies until you remove the last reference to it from the dict using 'del'
				#
				restarts.append(running_procs[rp].args[1])
				running_procs[rp].wait()
				kill_pids.append(rp)
				
		for p in kill_pids:
			del running_procs[p]
	
		for restart in restarts:
			start_crawler(restart)
			
		print("Sleeping for 10")
		time.sleep(10)
			

main()	
