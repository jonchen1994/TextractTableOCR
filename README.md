This is a Repository using AWS's textract functions to identify and capture tables in OCRable documents. Tool/workflow is able to identify tables from PDFs regardless of portrait/landscape orientation.
Depending on table type/desired output, further downstream post processing for prettifying extracted Blocks of data will be needed.

Input files are OCRable documents - Pdfs were the primary input here. 
Output: Folder full of individual and numbered CSV tables that follow sequence of original input document.

What's here:
* Common_Functions.py -  Handles some S3 interactions including paginating through buckets to identify what has been OCRd and what has not.
* OCR_Functions.py - Contains functions used in OCR.py for digesting the Blocks of raw data outputted by AWS' OCR function.
* OCR.py - Makes async calls to AWSs textract tool, and then retrieves the data via JobID, before using OCR_Functions to parse out the data into CSVs for further downstream processing.

