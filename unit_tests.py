import unittest
from my_mpi_simulator import add_exclamations, add_question_marks

#UNIT tested function

class TestAddToString(unittest.TestCase):
    def test_exclamations(self):
        test_string = add_exclamations('hello world')
        self.assertEqual(test_string.execute(), 'hello world!!')


    def test_questionmarks(self):
        test_string2 = add_question_marks('hello world')
        self.assertEqual(test_string2.execute(), 'hello world??')

if __name__ == '__main__':
    unittest.main()
else:
    print('error')