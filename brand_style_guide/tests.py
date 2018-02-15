#!/usr/bin/python

import unittest
from code_sample import brand_guide_number_replacement_formatter, is_in_range, first_word_in_sentence, replace_zero_through_ninty_nine, replace_single, replace_tens_column, replace_teen_special_cases, format_special_cases

class QuickStartTest(unittest.TestCase):
    test_values = {0:"zero", 
                   1:"one",  
                   2:"two",  
                   3:"three",  
                   4:"four",  
                   5:"five",  
                   6:"six",  
                   7:"seven",  
                   8:"eight",  
                   9:"nine",  
                   10:"ten",  
                   11:"eleven",  
                   12:"twelve",  
                   13:"thirteen",  
                   14:"fourteen",  
                   15:"fifteen",  
                   16:"sixteen",  
                   17:"seventeen",  
                   18:"eighteen",  
                   19:"nineteen",  
                   20:"twenty",  
                   21:"twenty-one",  
                   22:"twenty-two",  
                   23:"twenty-three",  
                   24:"twenty-four",  
                   25:"twenty-five",  
                   26:"twenty-six",  
                   27:"twenty-seven",  
                   28:"twenty-eight",  
                   29:"twenty-nine",  
                   30:"thirty",  
                   31:"thirty-one",
                   32:"thirty-two",  
                   33:"thirty-three",  
                   34:"thirty-four",  
                   40:"fourty",
                   42:"fourty-two",  
                   50:"fifty",  
                   53:"fifty-three",
                   60:"sixty",  
                   69:"sixty-nine",  
                   70:"seventy", 
                   74:"seventy-four",  
                   80:"eighty",  
                   88:"eighty-eight",  
                   90:"ninety",  
                   99:"ninety-nine",
                   }


    def test_example(self):
        #Tests will run.
        self.assertEqual(True, True)


    def test_provided_example(self):
        #Tests provided example input and output from instructions.
        expected = "The two little men bought three fish."
        actual = brand_guide_number_replacement_formatter("The 2 little men bought 3 fish.")
        self.assertEqual(expected, actual)

    def test_brand_guide_number_replacement_formatter(self):
        expected = "Ninety-Nine is capitalized when it is the first word in the paragraph. When it is in the middle of a sentence, ninety-nine is not capitalized. Ninety-Nine is also capitalized after a period. Not capitalized when the value is presented last, like this: ninety-nine. Handles floats 99.99 also."
        actual = brand_guide_number_replacement_formatter("99 is capitalized when it is the first word in the paragraph. When it is in the middle of a sentence, 99 is not capitalized. 99 is also capitalized after a period. Not capitalized when the value is presented last, like this: 99. Handles floats 99.99 also.")
        self.assertEqual(expected, actual)


    def test_format_special_cases(self):
        #Valid input number with punctuation
        expected = "ninety-nine."
        actual = format_special_cases("99.", 1, 99)
        self.assertEqual(expected, actual)
        #Valid input float
        expected = "77.77"
        actual = format_special_cases("77.77", 1, 99)
        self.assertEqual(expected, actual)


    def test_is_in_range(self):
        #Valid input test lower bound
        self.assertTrue(is_in_range(2, 2, 10))
        #Valid input test upper bound
        self.assertTrue(is_in_range(10, 2, 10))
        #Valid input test middle range
        self.assertTrue(is_in_range(5, 2, 10))
        #Valid input test below range
        self.assertFalse(is_in_range(1, 2, 10))
        #Valid input test above range
        self.assertFalse(is_in_range(11, 2, 10))


    def test_first_word_in_sentence(self):
        token_list = ["19", "period.", "7", "8", "word."]
        #First word of sample.
        self.assertTrue(first_word_in_sentence(token_list, 0))
        #First word after a period.
        self.assertTrue(first_word_in_sentence(token_list, 2))
        #Neither the first word of a sample nor follows a period.
        self.assertFalse(first_word_in_sentence(token_list, 3))


    def test_replace_zero_through_ninty_nine(self):
        #Valid inputs 0-99
        #Will only test selected values present in the test_values.
        for key, value in self.test_values.items():
            expected = value
            actual = replace_zero_through_ninty_nine(key)
            self.assertEqual(expected, actual)


    def test_replace_single(self):
        #Valid inputs 1-9
        for x in range(1, 10):
            expected = self.test_values[x]
            actual = replace_single(x)
            self.assertEqual(expected, actual)
        #Valid input 0
        expected = ""
        actual = replace_single(0)
        self.assertEqual(expected, actual)


    def test_replace_tens_column(self):
        #Valid inputs 2-9
        for x in range(2, 10):
            expected = self.test_values[int('{0}{1}'.format(x, 0))] #example of the format: '20', '30', etc
            print(expected)
            actual = replace_tens_column(x)
            self.assertEqual(expected, actual)


    def test_replace_teen_special_cases(self):
        #Valid inputs 10-19
        for x in range(10, 20):
            expected = self.test_values[x]
            actual = replace_teen_special_cases(x)
            self.assertEqual(expected, actual)
        

  
#TODO Performance tests could be added if required
#TODO Invalid input tests could be added.  Function currently assumes valid input.
