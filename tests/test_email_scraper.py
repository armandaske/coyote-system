import sys
import os
from bs4 import BeautifulSoup

# Path to your HTML file
file_name = r'C:\Users\Dell-G3\.spyder-py3\coyote-system\tests\airbnb_booking_new.html'

# Add the parent directory of the email_scraper folder to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'email-processor')))
import email_scraper_helpers as esh


# Read the file and parse it with BeautifulSoup
with open(file_name, 'r', encoding='utf-8') as file:
    soup = BeautifulSoup(file, 'html.parser')

#print(esh.abnb_extract_booking_info_new(soup))
#print(esh.abnb_extract_booking_info(soup))     
#print(esh.fh_extract_booking_info(soup))   
#print(esh.fh_extract_rebooking_info(soup))   
#print(esh.fh_extract_cancellation_info(soup))   

#def test_fh_extract_rebooking_info():
#    assert esh.fh_extract_rebooking_info('example input') == 'expected output'

#def test_fh_extract_cancellation_info():
#    assert esh.fh_extract_cancellation_info('example input') == 'expected output'