import unittest


# The function to be tested
def add_numbers(a, b):
    return a + b


# The unit test
class TestAddNumbersFunction(unittest.TestCase):
    def test_add_numbers(self):
        # Test case: adding positive numbers
        result = add_numbers(3, 4)
        self.assertEqual(result, 7)  # Assert that the result is equal to 7
        # Test case: adding negative numbers
        result = add_numbers(-2, -5)
        self.assertEqual(result, -7)  # Assert that the result is equal to -7

        # Test case: adding a positive and a negative number
        result = add_numbers(10, -3)
        self.assertEqual(result, 7)  # Assert that the result is equal to 7


if __name__ == '__main__':
    unittest.main()
