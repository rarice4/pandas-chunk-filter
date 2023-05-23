from google.colab import drive, files # specific to Google Colab
import pandas as pd

# //////////

# View sample data
# url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'
# df = pd.read_csv(url,compression='gzip', sep='\t', on_bad_lines='skip', nrows=100)
# df.head()

# ///////////

#FILTER ON SEATTLE METRO ZIPCODES - need to bring in related zipcode table to filter further down on city of seattle zipcodes only
# Specify the url path to dataset file
dataset_path = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz'

# Filter zipcodes down to seattle metro zip codes
filter_column = "parent_metro_region"

filter_value = "Seattle, WA"

# read data in chunks to conserve RAM
chunk_size = 10_000  

df = pd.read_csv(dataset_path, chunksize=chunk_size, compression='gzip', sep='\t', on_bad_lines='skip')

# Create an empty list to store the filtered chunks
filtered_chunks = []

# Iterate over the chunks and filter on filter_value = "Seattle, WA"
for chunk in df:
    filtered_chunk = chunk[chunk[filter_column] == filter_value]
    filtered_chunks.append(filtered_chunk)

# Concatenate the filtered chunks into a single DataFrame
filtered_data = pd.concat(filtered_chunks, ignore_index=True)

#read head to check data
# print(filtered_data.head())
# print(filtered_data.keys())

#zip code column is named "region" so it needs to be renamed zip_code
filtered_data = filtered_data.rename(columns={"region": "zip_code"})
#create csv to use as object for read_csv
filtered_data.to_csv('filtered_data.csv',index=False)

# ////////////
#need to remove prefix "Zip Code: " from every row in the zip_code column to clean for use
# Specify the column name for filtering
filter_column = 'zip_code'

# Specify the prefix to remove
prefix_to_remove = "Zip Code: "  


chunk_size = 10_000  
data_iterator = pd.read_csv('./filtered_data.csv', chunksize=chunk_size)

# Create an empty list to store the filtered chunks
filtered_chunks = []


# Iterate over the chunks and remove the prefix from zip_code rows
for chunk in data_iterator:
    
    # Remove the prefix "Seattle, WA - " from the zip_code column 
    chunk[filter_column] = chunk[filter_column].str.lstrip(prefix_to_remove)
    filtered_chunks.append(chunk)

# Concatenate the filtered chunks into a single DataFrame
cleaned_data = pd.concat(filtered_chunks)

# create new csv and download
cleaned_data.to_csv('seattle-real-estate-by-zipcode.csv',index=False)
files.download('seattle-real-estate-by-zipcode.csv')