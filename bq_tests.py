import unittest, os
import bq_module

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))

class BigQueryTests(unittest.TestCase):

	def __init__(self, *args, **kwargs) :
		# Deal with override 
		super(BigQueryTests, self).__init__(*args, **kwargs)

		# Init classes to be tested
		self.bqHandler = bq_module.BQHandler(ROOT_DIR + "/bt_credentials.json")

		# Date range known to be valid
		validStart = 1990
		validEnd = 1991

		# Keys that must be available in all list maps
		self.assertKeys = ["temp_max", "temp_min", "da", "mo", "year", "state"]

		# Get test data with valid input
		self.testDataValidInput = self.bqHandler.maxMinTemps(validStart, validEnd)

	def testBQTExceptionInvalidDateRange(self) :
		"""Test Big Query function breaks on invalid date range"""
		with self.assertRaises(ValueError) :
			self.bqHandler.maxMinTemps(validStart, validStart - 1)

	def testBQTempsValidInputResultIsList(self) :
		"""Test Big Query valid input returns list type"""
		self.assertIsInstance(self.testDataValidInput, list)

	def testBQTempsValidInputResultListIsLenG0(self) :
		"""Test Big Query valid input returns list length > 0"""
		self.assertTrue(len(self.testDataValidInput) > 0)

	def testBQTempsValidInputResultListElelemtsAreDicts(self) :
		"""Test Big Query valid input returns list of all maps"""
		for e in self.testDataValidInput :
			self.assertIsInstance(e, dict)

	def testBQTempsValidInputResultListElelemtsHaveValidKeys(self) :
		"""Test Big Query valid input returns maps all with the correct keys"""
		for e in self.testDataValidInput :
			keysFound = 0
			for key in self.assertKeys :
				if key in e :
					keysFound += 1
			self.assertTrue(keysFound == len(self.assertKeys))

if __name__ == '__main__':
    unittest.main()