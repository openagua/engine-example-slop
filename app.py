import platform
from tasks import app

if __name__ == '__main__':
    args = ['-A', 'tasks', 'worker', '-l', 'INFO']
    if platform.system() == 'Windows':
        args.extend(['-P', 'solo'])
    app.start(args)
