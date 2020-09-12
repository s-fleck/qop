from importlib import reload
from qcp import converters
from qcp import tasks

reload(tasks)
reload(converters)

f = "../tests/test_Converter/16b.flac"


c = converters.OggConverter()
t = tasks.ConvertTask(src=f, dst="/home/hoelk/test.ogg", converter=c, validate = False)


q = tasks.TaskQueue()

q.put(t)
q.run()