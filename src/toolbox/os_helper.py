import os, psutil

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
