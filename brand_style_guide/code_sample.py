#!/usr/bin/python
from __future__ import print_function
import re

#Instructions
# This exercise is meant to test your technical skills and level of Python familiarity.
#Please complete the task below.
#This exercise should take no more than 1 hour.
#
#Task
#In accordance with our brand styleguide, we need to convert all instances of whole numerals between 1 and 99 with the written form of the numbers in our copy.
#
#Write a function in Python that accepts a string of text and returns the string with its numerals replaced.
#For example, "The 2 little men bought 3 fish."  would be transformed to "The two little men bought three fish."
#If the number is the beginning of a sentence, it should be properly capitalized.



def brand_guide_number_replacement_formatter(string_to_format):
    """
    Param string_to_format must be a string.
    Returns a string with numbers 1 to 99 replaced with the English representation of the number.
        Numbers that begin sentences will be capitalized.
    """
    #Assumes input validation has already taken place.
    #Assumes that the string is not going to exceed memory capacity.
    #The marketer has worse problems if their marketing content is so long it exceeds memory.
    lower_bound = 1
    upper_bound = 99
    formatted_list = string_to_format.split(" ")
    for idx, token in enumerate(formatted_list):
        replacement_value = token #if nothing else, we use the token
        try:
            int_token = int(token) 
            replacement_value = format_number(int_token, lower_bound, upper_bound)
        except ValueError:
            #this means that the coversion to int failed, still need to check if it contains numbers
            if contains_numbers(token):
                replacement_value = format_special_cases(token, lower_bound, upper_bound)
        if first_word_in_sentence(formatted_list, idx):
            replacement_value = replacement_value.title()
        formatted_list[idx] = replacement_value
    return " ".join(formatted_list)


def contains_numbers(token):
    """
    Param token must be a string
    Returns boolean indicating whether the string contains 0-9
    """
    if re.search("[0-9]", token):
        return True
    return False

    
def format_special_cases(token_to_format, lower_bound, upper_bound):
    """
    Param token_to_format must be an string.
    Param lower_bound must be an integer
    Param upper_boud must be an integer
    Return a string representing edge case formatting on token_to_format.
    """
    #TODO consider how to clean up edge cases
    build_list = []
    char_list = list(token_to_format)
    while len(char_list) > 0:
        current_chars = char_list.pop(0)
        if not contains_numbers(current_chars):
            build_list.append(current_chars)
            continue
        while len(char_list) > 0 and contains_numbers(char_list[0]):
            current_chars += char_list.pop(0)
        if len(char_list) > 1 and char_list[0] == '.': #only be a float if more than two chars remain
           if contains_numbers(char_list[1]):
               current_chars += char_list.pop(0)
               while len(char_list) > 0 and contains_numbers(char_list[0]):
                   current_chars += char_list.pop(0)
               build_list.append(current_chars)
               continue
        build_list.append(format_number(int(current_chars), lower_bound, upper_bound))
    return "".join(build_list)


def format_number(int_token, lower_bound, upper_bound):
    """
    Param int_token must be an integer
    Param lower_bound must be an integer
    Param upper_boud must be an integer
    Returns a string of the formatted int_token.
    """
    if is_in_range(int_token, lower_bound, upper_bound):
        return replace_zero_through_ninty_nine(int_token)
    else:
        return str(int_token)
    

def is_in_range(integer_to_check, inclusive_lower_bound, inclusive_upper_bound):
    """
    Param integer_to_check must be an integer
    Param inclusive_lower_bound must be an integer.  Value is min value of the check.
    Param inclusive_upper_bound must be an integer.  Value is max value of the check.
    Returns a boolean based on a range check.
    Examples:
        is_in_range(2, 2, 10) returns True
        is_in_range(10, 2, 10) returns True
        is_in_range(1, 2, 10) returns False
    """
    if integer_to_check > inclusive_upper_bound or integer_to_check < inclusive_lower_bound:
        return False
    else:
        return True


def first_word_in_sentence(token_list, idx):
    """
    Param token_list is the tokenized representation of English sentence(s), broken on spaces.  Tokens include punctuation.
    Param idx is the index of the location being checked to determine if it is the first word of an English sentence.
    Returns True if a word is the first word of a sentence.
    A word is the first word in a sentence when one of the following is true:
        1) The token is the first word in the string
        2) If the token before the current token ended with a period, that is the end of a sentence
    """
    if idx is 0:
        return True
    elif token_list[idx-1][-1] == ".":
        return True
    else:
        return False


def replace_zero_through_ninty_nine(number_to_replace):
    """
    Param number_to_replace must be an integer (i) that satisfies 0 <= i <= 99
    Returns a string representing the equivalent English word for the number
    """
    #though zero is not currently a requirement, this prevents a future error in replacement of zero with "" if the range was expanded
    if number_to_replace == 0: 
        return "zero"
    if number_to_replace <= 19 and number_to_replace >= 10:
        return replace_teen_special_cases(number_to_replace)
    replacement_list = []
    num_to_replace_string = str(number_to_replace)
    singles_column = int(num_to_replace_string[-1])
    if len(num_to_replace_string) > 1:
        tens_column = int(num_to_replace_string[-2])
        replacement_list.append(replace_tens_column(tens_column))
        if singles_column is not 0:
            replacement_list.append("-")
    replacement_list.append(replace_single(singles_column))
    return "".join(replacement_list)


def replace_single(number_to_replace):
    """
    Param number_to_replace must be an integer (i) that satisfies 0 <= i <= 9
    Returns a string representing the equivalent English word for the single digit column of a number
    Passing in the integer 0 returns an empty string for pairing with column descriptors, and is not intended to replace the actual number zero.
    """
    single_translation = {0:"", 1:"one", 2:"two", 3:"three", 4:"four", 5:"five", 6:"six", 7:"seven", 8:"eight", 9:"nine"}
    return single_translation[number_to_replace]


def replace_tens_column(number_to_replace):
    """
    Param number_to_replace must be an integer (i) that satisfies 2 <= i <= 9
    Returns a string representing the equivalent English word for the tens place holder
    """
    tens_translation = {2:"twenty", 3:"thirty", 4:"fourty", 5:"fifty", 6:"sixty", 7:"seventy", 8:"eighty", 9:"ninety"}
    return tens_translation[number_to_replace]


def replace_teen_special_cases(number_to_replace):
    """
    Param number_to_replace must be an integer (i) that satisfies 10 <= i <= 19
    Returns a string representing the equivalent English word for that number
    """
    teen_special_case_translation = {10:"ten", 11:"eleven", 12:"twelve", 13:"thirteen", 14:"fourteen", 15:"fifteen", 16:"sixteen", 17:"seventeen", 18:"eighteen", 19:"nineteen"}
    return teen_special_case_translation[number_to_replace]


if __name__ == "__main__":
    print("Sample sentence:")
    print(brand_guide_number_replacement_formatter("The 2 little men bought 3 fish."))
    print("Printing value checks for 0 through 101.  Expects no replacement for 0 and 101.")
    for x in range(101):
        print("{0} produces:  {1}".format(x, brand_guide_number_replacement_formatter("{0} is capitalized when it is the first word in the paragraph. When it is in the middle of a sentence, {0} is not capitalized. {0} is also capitalized after a period. Not capitalized when the value is presented last, like this: {0}. Handles Floats {0}.{0} also.".format(str(x)))))


