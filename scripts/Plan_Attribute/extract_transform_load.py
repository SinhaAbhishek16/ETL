from sqlalchemy import create_engine, MetaData, Table, Column, INTEGER, TEXT, NUMERIC
engine = create_engine('sqlite:///etl.db', echo = True)
meta = MetaData()

Network = Table(
   'Network', meta,
   Column('BusinessYear', TEXT),
   Column('StateCode', TEXT),
   Column('IssuerId', TEXT),
   Column('SourceName', TEXT),
   Column('VersionNum', NUMERIC),
   Column('ImportDate', INTEGER),
   Column('IssuerId2', NUMERIC),
   Column('StateCode2', TEXT),
   Column('NetworkName', TEXT),
   Column('NetworkId', TEXT),
   Column('NetworkURL', TEXT),
   Column('RowNumber', TEXT),
   Column('MarketCoverage', TEXT),
   Column('DentalOnlyPlan', TEXT),

)
meta.create_all(engine)

conn = engine.connect()



'''
sql = """
CREATE TABLE Network(
 BusinessYear TEXT,
 StateCode TEXT,
 IssuerId TEXT,
 SourceName TEXT,
 VersionNum NUMERIC,
 ImportDate INTEGER,
 IssuerId2 NUMERIC,
 StateCode2 TEXT,
 NetworkName TEXT,
 NetworkId TEXT
)"""

cur.execute(sql)
print("Table has been created")

conn.commit()
conn.close()
'''

with open('../../input/year/2014/Network.csv','r') as file:
    no_records = 0
    for row in file:
        conn.execute("INSERT INTO Network VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.split(","))
        conn.commit()
        no_records += 1

conn.close()
print('\n{} Records Transferred'.format(no_records))





