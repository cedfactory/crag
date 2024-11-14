import os, sys, platform, subprocess, psutil

g_os_platform = platform.system()
g_python_executable = ""

#
def git_pull():
	if g_os_platform == "Windows":
		return True

	command = ["git", "pull"]
	try:
		result = subprocess.run(command, stdout=subprocess.PIPE)
	except Exception as e:
		print("Problem while updating the repo")
		return False
	return True

#
def source_activate():
	return True

	command = ["source", ".venv/bin/activate"]
	try:
		result = subprocess.run(command, stdout=subprocess.PIPE)
	except Exception as e:
		print("Problem during source activation")
		return False
	return True

#
def launch_strategy(xml_file):
	command = []
	log_file = "alcorak_" + xml_file + ".log"
	if g_os_platform == "Windows":
		command = "start /B python -u alcorak.py --start {} > {}".format(xml_file, log_file)
		print("command : ", command)
		os.system(command)
	elif g_os_platform == "Linux":
		#command = "source .venv/bin/activate && nohup python alcorak.py --start {} > {} &".format(xml_file, log_file)
		command = "nohup python alcorak.py --start {} > {} &".format(xml_file, log_file)
		print("command : ", command)
		subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)

#
def select_strategy_to_start():
	directory = os.getcwd()
	strategies = []
	for file_name in os.listdir(directory+"/conf"):
		if file_name.endswith('AWS.xml'):
			strategies.append(file_name)

	print("Available strategies")
	for index, value in enumerate(strategies):
		if value == "settings.xml":
			continue

		print(str(index) + " : " + value)

	print("Select the strategy to start (-1 to quit)")

	id = -2
	while id < -1 or id >= len(strategies):
		try:
			id_str = input("> ").strip().lower()
			id = int(id_str)
			if id == -1:
				return ""
		except ValueError:
			print(f"! Error: '{id_str}' is not a valid integer.")
		except TypeError:
			print("! Error: The input type is invalid, cannot convert to integer.")
		if id < 0 or id >= len(strategies):
			print(f"! Error: select a valid strategy by its index.")
	
	return strategies[id]

def select_strategy_to_stop():
	current_pid = os.getpid()
	pids = []
	i = 0
	for process in psutil.process_iter(['pid', 'name', 'cmdline']):
		if not 'name' in process.info:
			continue
		try:
			if 'python' in process.info['name']:
				if int(process.info['pid']) != current_pid:
					print(f"{i} : Command {' '.join(process.info['cmdline'])}")
					pids.append(process.info['pid'])
					i += 1
		except (psutil.NoSuchProcess, psutil.AccessDenied):
			# Skip processes that may have terminated or where access is restricted
			continue
	
	if len(pids) == 0:
		print("No python process to kill. Bye.")
		return

	pid = -2
	print("Select the process to stop (-1 to quit)")
	while pid < -1 or pid >= len(pids):
		try:
			pid_str = input("> ").strip().lower()
			pid = int(pid_str)
			if pid == -1:
				return
		except ValueError:
			print(f"! Error: '{pid_str}' is not a valid integer.")
		except TypeError:
			print("! Error: The input type is invalid, cannot convert to integer.")
		if pid < 0 or pid >= len(pids):
			print(f"! Error: select a valid id by its index.")


	p = psutil.Process(pids[pid])
	p.terminate()
	p.wait()
	print(f"Process terminated.")

def usage():
	print("start or stop ???")


if __name__ == "__main__":
	print('''
	  ______                __     _____ 
	 /_  __/______  _______/ /_   / ___/___  _________ ____
	  / / / ___/ / / / ___/ __/   \\__ \\/ _ \\/ ___/ __ `/ _ \\
	 / / / /  / /_/ (__  ) /_    ___/ /  __/ /  / /_/ /  __/
	/_/ /_/   \\__,_/____/\\__/   /____/\\___/_/   \\__, /\\___/ 
		                		   /____/
	''')
	print("# Platform :", g_os_platform)
	g_python_executable = sys.executable
	print("# Python executable :", g_python_executable)
	print("\n")
	
	print("Available actions :\n- 'start' to start a strategy\n- 'stop' to stop a running strategy")
	action = input("> ").strip().lower()
	print("\n")
	
	if action == "start":
		xmlfile = select_strategy_to_start()
		if xmlfile != "":
			git_pull() and source_activate() and launch_strategy(xmlfile)
		
	elif action == "stop":
		xmlfile = select_strategy_to_stop()
	else:
		usage()
