import unittest
from test.utils_test import UtilsTest

unittest.main()

suite = unittest.TestLoader().loadTestsFromModule(UtilsTest)
unittest.TextTestRunner(verbosity=2).run(suite)