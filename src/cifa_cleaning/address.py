import re
import pandas as pd

file_path = r"C:\Users\jfeaga619\OneDrive - Comcast\3-Resources\Reporting\Field List Master.csv"

df = pd.read_csv(file_path)

print(df.columns)

def extract_address(address):
    #Match street address loosely
    street_pattern = r"\d+\s[A-Z][a-z]+\s[A-Z][a-z]+"  # Match street address (number, street name, street type)

    #Match City Name
    city_pattern = r"([A-Z][a-z]+(?:\s[A-Z][a-z]+)*)"

    #Match State Abbreviation    # Match state abbreviations (all 50 states and DC)
    state_pattern = r"\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b"
    
    # Match ZIP code (5-digit or ZIP+4)
    zip_pattern = r"\b\d{5}(-\d{4})?\b"

    # Extract Results
    street = re.search(street_pattern, address).group()
    city = re.search(city_pattern, address).group()
    state = re.search(state_pattern, address).group()
    zip_code = re.search(zip_pattern, address).group()

    return street, city, state, zip_code

df["Street"], df["City"], df["State"], df["Zip Code"] = zip(*df["Address"].apply(extract_address))

print(df.head())