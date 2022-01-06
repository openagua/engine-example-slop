import platform
from tasks import app

args = ['-A', 'tasks', 'worker', '-l', 'INFO', '-n', 'SLOP']
if platform.system() == 'Windows':
    args.extend(['-P', 'solo'])
app.start(args)
