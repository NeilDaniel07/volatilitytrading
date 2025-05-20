import shutil
import urllib.request as request
from contextlib import closing
import csv

URLS = [
    {
        "url": "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt",
        "key": "Symbol"
    }
]

output_csv = "nasdaq_listed.csv"

for u in URLS:
    local_filename = u['url'].split("/")[-1]
    
    with closing(request.urlopen(u['url'])) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r, f)

    with open(local_filename, 'r') as f_in, open(output_csv, 'w', newline='') as f_out:
        reader = csv.reader(f_in, delimiter="|")
        writer = csv.writer(f_out)
        writer.writerow(['Ticker', 'Company'])
        
        for row in reader:
            if len(row) < 2 or '-' not in row[1]:
                continue
            ticker = row[0].strip()
            company = row[1].split(' -', 1)[0].strip()
            writer.writerow([ticker, company])
