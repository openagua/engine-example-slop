from tasks import app

app.start(['-A', 'tasks', 'worker', '-l', 'INFO', '-P' 'solo'])
