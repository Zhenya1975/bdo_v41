# from pyexcel.cookbook import merge_all_to_a_book
# # import pyexcel.ext.xlsx # no longer required if you use pyexcel >= 0.2.2 
# import glob
import openpyxl
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows


# merge_all_to_a_book(glob.glob("temp_data/iterations_df.csv"), "temp_data/output.xlsx")

# Where do you want the file?
df = pd.read_csv("temp_data/iterations_df.csv", low_memory=False)


# Create a workbook & a sheet
wb = openpyxl.Workbook(write_only=True)
ws = wb.create_sheet("Sheet1")

# Operate rowwise
# rows = openpyxl.utils.dataframe.dataframe_to_rows(df)
rows = dataframe_to_rows(df)
# 
# print(df)
i=0
lenght = len(list(rows))
# print(rows)
# for i,row in enumerate(rows):
#   print(row)
#   i=i+1
#   print(i, " из ", lenght)
#   ws.append(row)

for r in dataframe_to_rows(df, index=True, header=True):
  print(r)
  ws.append(r)

# Now save
wb.save("temp_data/output.xlsx")