import platform
from tasks import app

args = ['-A', 'tasks', 'worker', '-l', 'INFO', '-n', 'test']
if platform.system() == 'Windows':
    args.extend(['-P', 'solo'])
app.start(args)
