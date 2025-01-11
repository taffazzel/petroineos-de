from bs4 import BeautifulSoup
import requests
from src.main.constants import *

import requests
from bs4 import BeautifulSoup

# URL of the webpage containing the latest Excel file link
webpage_url = 'https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends'

# Local file name to save the downloaded file
local_filename = f'{landing_zone}/inputfile.xlsx'


def get_latest_file_url(webpage_url):
    """Scrape the webpage to find the latest file URL."""
    try:
        response = requests.get(webpage_url)
        response.raise_for_status()

        # Parse the HTML content of the page
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the link to the latest file (adjust based on the website's structure)
        # Example: Find an anchor (<a>) tag containing "report" or ".xlsx"
        latest_file_link = soup.find('a', href=True, text=lambda x: x and '.xlsx' in x)

        if latest_file_link:
            # Construct the full URL if the link is relative
            file_url = requests.compat.urljoin(webpage_url, latest_file_link['href'])
            print(f"Latest file URL found: {file_url}")
            return file_url
        else:
            print("No Excel file link found on the webpage.")
            return None
    except Exception as e:
        print(f"Error while fetching the latest file URL: {e}")
        return None


def download_file(url, local_filename):
    """Download a file from the given URL."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Save the file locally
        with open(local_filename, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print(f"File downloaded successfully: {local_filename}")
    except Exception as e:
        print(f"Error while downloading the file: {e}")


# Fetch the latest file URL
latest_file_url = get_latest_file_url(webpage_url)

# Download the file if a URL was found
if latest_file_url:
    download_file(latest_file_url, local_filename)
