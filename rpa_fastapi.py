import uvicorn
import os
import json
from io import BytesIO
from io import StringIO
import uuid
import zipfile
import gzip
from fastapi.datastructures import Default
import pandas as pd

#from deta import Deta
#from flask import Flask, render_template, request,send_file,send_from_directory
#from werkzeug.utils import secure_filename

import rpa_ident as rid

import pandera as pa

from typing import Optional, Union

from pydantic import BaseModel, Field

from fastapi import FastAPI, File, Form, UploadFile
from pandera.typing import DataFrame

#from pandera.typing.fastapi import UploadFile


class Transactions(pa.SchemaModel):
    id: pa.typing.Series[int]
    cost: pa.typing.Series[float] = pa.Field(ge=0, le=1000)

    class Config:
        coerce = True

class TransactionsOut(Transactions):
    id: pa.typing.Series[int]
    cost: pa.typing.Series[float]
    name: pa.typing.Series[str]

class TransactionsDictOut(TransactionsOut):
    class Config:
        to_format = "dict"
        to_format_kwargs = {"orient": "records"}        


class TransactionsParquet(Transactions):
    class Config:
        from_format = "parquet"

class TransactionsJsonOut(TransactionsOut):
    class Config:
        to_format = "json"
        to_format_kwargs = {"orient": "records"}

class ResponseModel(BaseModel):
    filename: str
    df: pa.typing.DataFrame[TransactionsJsonOut]


class RPA_MeasurementIN(pa.SchemaModel):
    #name: str
    time: pa.typing.Series[float] = pa.Field(ge=0)
    nstar: pa.typing.Series[float] = pa.Field(ge=0)
    sdash: pa.typing.Series[float] = pa.Field(ge=0)
    tempc: pa.typing.Series[float] = pa.Field(ge=0)

class RPA_MeasurementOUT(RPA_MeasurementIN):    
    hrate: pa.typing.Series[float] = pa.Field(ge=0)
    #gammad: pa.typing.Series[float] = pa.Field(ge=0)
    test_no: pa.typing.Series[int]

class RPA_Field(RPA_MeasurementOUT):
    gammap: pa.typing.Series[float] = pa.Field(ge=0)

class RPA_Model(BaseModel):
    id: int
    name: str
    paramA: float
    paramC: float
    paramn: float


app = FastAPI()
'''
@app.post("/panderafile/", response_model=ResponseModel)
def create_upload_file(
    file: UploadFile[DataFrame[TransactionsParquet]] = File(...),
):
    return {
        "filename": file.filename,
        "df": file.data.assign(name="foo"),
    }            
'''


@app.post("/transactions/", response_model=DataFrame[TransactionsDictOut])
def create_transactions(transactions: DataFrame[Transactions]):
    output = transactions.assign(name="foo")
    ...  # do other stuff, e.g. update backend database with transactions
    return output





@app.post('/rpa_fit/')
async def create_visco_fit(
    df: DataFrame[RPA_MeasurementIN],
    lowerTC: float = Form(80,ge=40,le=130, description = 'Lower Temperature Boundary in °C used for Fitting'),
    upperTC: float = Form(140,ge=90,le=200, description = 'upper Temperature Boundary in °C used for Fitting'),
    ):
    res = rid.fit_visco(df,lowert=lowerTC,uppert=upperTC)
    return res



@app.post('/upload_data/')
def create_data_frame(
        experiment: str = Form(...,description='id of experiment'),
        file_type: Union[str ,None]='html',
        file_id: str = Form(...),
        data_file: UploadFile = File(...),
        ):
    
    #decoded = base64.b64decode(data_file.file)
    #decoded = io.StringIO(decoded.decode('utf-8'))
    
    #print(pd.read_csv(data_file.file, sep='\t'))
    print(data_file.filename)
    basename=os.path.basename(data_file.filename) # filename without path
    base,ext = os.path.splitext(basename) # filename without extension
    print(base,ext)
    btxt = data_file.file.read()#.decode()   
    if ext.lower() in ['.htm','.html']:
        #with open(htmlfile,'rb') as fd: #encoding='utf-8'
        #    btxt=fd.read()
        #    print(len(btxt))
        #
        #print(dir(data_file.file))
        txt =btxt.decode()
        #print(type(txt))
        #print(len(txt),txt[:100])
        df = rid.html2df(txt)
    
    #txt = gzip.decompress(btxt).decode('utf-8')

    elif ext.lower() =='.csv':
        
        df = pd.read_csv(BytesIO(btxt), encoding='utf-8')

    elif ext.lower() in ['.zip']:
        #### TO DO, must be written to temporary file and then reloaded (python bug)
        tempfilename = str(uuid.uuid4())
        with open(tempfilename,'wb') as fo:
            fo.write(btxt)
        #data_file.file.read()
        with zipfile.ZipFile(tempfilename) as thezip:
            print ( thezip.infolist())
            for file in thezip.infolist():
                with thezip.open(file) as fz:
                    txt = fz.read().decode()
        df = rid.html2df(txt)
        os.remove(tempfilename)
            #for zipinfo in thezip.infolist():
            #    with thezip.open(zipinfo) as thefile:
            #        yield zipinfo.filename, thefile
            #        print(thefile)

        #                with zipfile.ZipFile(io.BytesIO(file.read()), 'r') as zip:

    elif ext.lower() in ['.gzip']:
        #### TO DO
        with gzip.open(data_file.file,mode="rt") as fz:
            txt=fz.read()
            print(type(txt))
        df = rid.html2df(txt)


    try:
        print(df.info())
        res = create_visco_fit(df,80,140)
        print(res)
    except:
        print('nix')
    
    return {'filename': data_file.filename, 
            'experiment':experiment, 
            'file_type': file_type, 
            'file_id': file_id}


      
if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
   


