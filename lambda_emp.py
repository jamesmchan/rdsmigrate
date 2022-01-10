import pymongo
import pandas as pd 
import datetime
import boto3
import os



def lambda_handler(event, context):
    
    # read env variables for mongodb connection
    urlDb = os.environ['mongodburl']
    database = os.environ['database']
    collection = os.environ['collection']

    # configure pymongo connection
    myclient = pymongo.MongoClient(urlDb)
    mydb = myclient[database]
    mycol = mydb[collection]

    s3_client = boto3.client('s3')

    try:
        
        bucket_name  = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_name = event["Records"][0]["s3"]["object"]["key"]
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)

        df = pd.read_csv(resp['Body'], sep=',')

        with myclient.start_session() as session:
            for jdict in df.to_dict(orient='records'):
                # remove the field(s) with NaN value
                jdict = { k:v for k,v in jdict.items() if pd.notna(v) }

                op     = jdict['op']
                empno  = jdict['empno']
                deptno = jdict['deptno']

                if (op == 'I'):
                    del jdict['op']
                    del jdict['deptno']

                    try:
                        jdict['hiredate'] = datetime.datetime.strptime(jdict['hiredate'], '%d-%m-%Y')
                    except Exception as e:
                        pass

                    try:
                        mycol.update_one({ "deptno" : deptno }, { "$push" : { "emp" : { "$each" : [ jdict ] } } }, upsert=True, session=session)
            
                    except Exception as e:
                        print("Error insert empno", empno, " ", type(e), e)

                elif (op == 'U'):
                    del jdict['op']
                    del jdict['deptno']
                    del jdict['empno']

                    try:
                        jdict['hiredate'] = datetime.datetime.strptime(jdict['hiredate'], '%d-%m-%Y')
                    
                    except Exception as e:
                        pass

                    elemjdict = {"emp.$." + str(key): val for key, val in jdict.items()}

                    try:
                        mycol.update_one({ "deptno" : deptno, "emp" : { "$elemMatch" : { "empno" : empno } } }, { "$set" : elemjdict }, upsert=True, session=session)

                    except Exception as e:
                        print("Error insert empno", empno, " ", type(e), e)

                elif (op == 'D'):
                    try:
                        mycol.update_one({ "deptno" : deptno }, { "$pull" : { "emp" : { "empno" : empno } } }, session = session)

                    except Exception as e:
                        print("Error delete deptno", deptno, " ", type(e), e)
                else:
                    print("Error : Unknown Op code", op)

    except Exception as err:
        print(err)
