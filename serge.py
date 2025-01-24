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

def launch_simon():
	if g_os_platform == "Windows":
		command = "start /B python simon > simon.log"
		print("command : ", command)
		os.system(command)
	elif g_os_platform == "Linux":
		command = "nohup python simon.py > simon.log &"
		print("command : ", command)
		subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)

def launch_pulsar():
	if g_os_platform == "Windows":
		command = "start /B python pulsar conf/pulsar.csv > pulsar.log"
		print("command : ", command)
		os.system(command)
	elif g_os_platform == "Linux":
		command = "nohup python pulsar.py conf/pulsar.csv > pulsar.log &"
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

def get_python_processes():
	current_pid = os.getpid()
	processes = []
	for process in psutil.process_iter(['pid', 'name', 'cmdline']):
		if not 'name' in process.info:
			continue
		try:
			if 'python' in process.info['name']:
				if int(process.info['pid']) != current_pid:
					processes.append({"pid": process.info['pid'],
								"command": ' '.join(process.info['cmdline'])})
		except (psutil.NoSuchProcess, psutil.AccessDenied):
			# Skip processes that may have terminated or where access is restricted
			continue

	return processes

def display_python_processes():
	processes = get_python_processes()
	if len(processes) == 0:
		print("No python process")
		return

	for index, process in enumerate(processes):
		print("{} : Command {}".format(index, process["command"]))

def select_strategy_to_stop():
	all_processes = get_python_processes()

	# keep only alcorak processes
	processes = [process for process in all_processes if 'toto' in process['command']]

	if len(processes) == 0:
		print("No python process to kill. Bye.")
		return

	for index, process in enumerate(processes):
		print("{} : Command {}".format(index, process["command"]))


	id = -2
	print("Select the process to stop (-1 to quit)")
	while id < -1 or id >= len(processes):
		try:
			id_str = input("> ").strip().lower()
			id = int(id_str)
			if id == -1:
				return
		except ValueError:
			print(f"! Error: '{id_str}' is not a valid integer.")
		except TypeError:
			print("! Error: The input type is invalid, cannot convert to integer.")
		if id < 0 or id >= len(processes):
			print(f"! Error: select a valid id by its index.")


	p = psutil.Process(processes[id]["pid"])
	p.terminate()
	p.wait()
	print(f"Process terminated.")

def display_os_stats():

	print("Large files :")
	size_in_mb = 1
	size_in_bytes = size_in_mb * 1024 * 1024
	current_directory = os.getcwd()
	for file in os.listdir(current_directory):
		if not file.endswith("csv"):
			continue

		filepath = os.path.join(current_directory, file)
		if os.path.isfile(filepath):
			try:
				file_size = os.path.getsize(filepath)
				if file_size > size_in_bytes:
					print("{} - {:.2f} Mo".format(filepath, file_size / (1024 * 1024)))
			except OSError as e:
				print("Error with {}: {}".format(filepath, e))

	print("")
	print("Disk usage :")
	path = "/"
	if g_os_platform == "Windows":
		path = "C:\\"
	disk_info = psutil.disk_usage(path)
	print(f"  Espace total: {disk_info.total / (1024 ** 3):.2f} Go")
	print(f"  Espace utilisé: {disk_info.used / (1024 ** 3):.2f} Go")
	print(f"  Espace disponible: {disk_info.free / (1024 ** 3):.2f} Go")


def usage():
	print("start / running / stop / stats / quit ???")

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

	print('''Available actions :
	- 'start' to start a strategy
	- 'running' to display the running python processes
	- 'stop' to stop a running strategy
	- 'stats' to display large files and memory info
	- 'simon' to run simon
	- 'pulsar' to run pulsar
	- 'quit' to quit''')
	action = input("> ").strip().lower()
	print("\n")

	if action == "start":
		xmlfile = select_strategy_to_start()
		if xmlfile != "":
			git_pull() and source_activate() and launch_strategy(xmlfile)

	elif action == "running":
		display_python_processes()

	elif action == "stop":
		xmlfile = select_strategy_to_stop()

	elif action == "stats":
		display_os_stats()

	elif action == "simon":
		launch_simon()

	elif action == "pulsar":
		launch_pulsar()

	elif action == "quit":
		pass

	else:
		usage()
