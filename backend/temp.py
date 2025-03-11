from fastapi import FastAPI, File, UploadFile
import pandas as pd
import uvicorn
from app.config.settings import settings
app = FastAPI()

@app.post("/upload-file/")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Save the uploaded file temporarily
        file_location = settings.DATA_UPLOAD_FOLDER + f"temp_{file.filename}"
        with open(file_location, "wb") as f:
            f.write(await file.read())

        # Read the Excel file
        df = pd.read_excel(file_location, engine="openpyxl")
        print("Data from Excel file:")
        print(df)

        # Return the data as a response (optional)
        return {"message": "File processed successfully", "data": df.to_dict()}
    except Exception as e:
        return {"error": str(e)}
        
# curl -F "file=@/../../Data/salesrecords.xlsx" http://127.0.0.1:8000/upload-file/
# "C:\Users\patel\Downloads\ETL-StartUpProject\Data\salesrecords.xlsx"