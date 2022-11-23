# import os, sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.log import Log # noqa: E402

class Foo:
    Log.info("Can access")
    pass

Foo()