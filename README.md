# The purpose of this python script is to upload pesticide data to a database.
### There are instances where rows are misformated or missing key information like the product number, date, or the prodno is not recognized. Thus, there is a try/except statement to save those specific rows.
### If your data is formated OK, you can skip the try/except and iterrows and upload the entire df at once.
