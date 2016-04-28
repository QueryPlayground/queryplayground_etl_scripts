import rethinkdb as r
import os
r.connect( "localhost", 28015).repl()
try:
    r.db_create('queryplayground').run()
    r.db('queryplayground').table_create('socrata_datasets').run()
    r.db('queryplayground').table_create('third_party_creds').run()
    socrata_app_token = raw_input('Your Socrata app token\n')
    r.db('queryplayground').table_create('third_party_creds').insert({"id": "socrata", "app_token": socrata_app_token}).run()
except:
    pass
import requests
app_token = r.db('queryplayground').table('third_party_creds').get('socrata').run()['app_token']
while True:
    for dataset in r.db('queryplayground').table('socrata_datasets').run():

            if not 'socrata_created_at' in dataset:
                url = 'https://%s/resource/%s.csv?$select=:*,*&$limit=100000000' % (dataset['domain'], dataset['datasetid'])
            else:
                socrata_created_at = dataset['socrata_created_at']
                socrata_updated_at = dataset['socrata_updated_at']
                url = 'https://data.seattle.gov/resource/pu5n-trf4.json?$select=:*,*&$limit=2000000&$where=:created_at%%20>%%20"%s"%%20OR%%20:updated_at%%20>%%20"%s"&$$app_token=%s' % (socrata_created_at,socrata_updated_at,app_token)
            req = requests.get(url, stream=True)

            with open(local_filename, 'wb') as f:
                for chunk in req.iter_content(chunk_size=1024): 
                    if chunk: # filter out keep-alive new chunks
                        f.write(chunk)
            url = 'https://%s/resource/%s.json?' % (dataset['domain'], dataset['datasetid'])
            url += '$order=:created_at DESC&$limit=1&$select=:created_at&$$app_token=' + app_token
            print url
            dataset['socrata_created_at'] = requests.get(url).json()[0][':created_at']
            url = 'https://%s/resource/%s.json?' % (dataset['domain'], dataset['datasetid'])
            url += '$order=:updated_at DESC&$limit=1&$select=:updated_at&$$app_token=' + app_token
            print url
            dataset['socrata_updated_at'] = requests.get(url).json()[0][':updated_at']
            r.db('queryplayground').table('socrata_datasets').update(dataset).run()
            local_filename
            newline = os.linesep # Defines the newline based on your OS.

            source_fp = open(local_filename, 'r')
            target_fp = open('2'+local_filename, 'w')
            first_row = True
            for row in source_fp:

                if first_row:
                    row = row.replace(':', 'socrata_').replace('@', '_')
                    headers = row.strip().split(',')

                if (row.strip('\n').strip()):
                    if not first_row:
                        target_fp.write('\n')
                    target_fp.write(row.strip('\n').strip())
                #print first_row, row
                first_row = False
            source_fp.close()
            target_fp.close()
            schema = []
            for col in headers:
                schema.append({"name": col.strip('"'), "type": "string", "mode": "nullable"})
            import json
            with open('schema.json', 'w') as f:
                f.write(json.dumps(schema))
            import json
            cmd = 'bq load --apilog=- --schema=schema.json --skip_leading_rows=1 fromsocrata.%s %s' % (dataset['id'], '2'+local_filename)
            print cmd
            os.system(cmd)
            os.system('rm 2%s; rm %s' % (local_filename, local_filename))
    
