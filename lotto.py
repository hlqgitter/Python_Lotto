import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import psycopg2     # postgresSQL
from psycopg2.extras import execute_values
from psycopg2.extras import DictCursor, RealDictCursor
import zipfile
import csv 

DATABASE_NAME = "postgres"
TABLE_NAME = 'gewinnzahlen_eurojackpot'
temp_path_download = os.path.join(os.getcwd(), "download")
filename_eurojackpot = "eurojackpot.txt"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'}

def connect2psql():
    """Connect to the postgresSQL Databaase"""
    #print("Trying to access postgresSQL database lotto_informer")
    return psycopg2.connect(database=DATABASE_NAME, user="postgres", password="dualipa", host="127.0.0.1", port="5432")

def download_eurojackpot_file():
    url = "https://www.lotto-bayern.de/eurojackpot/gewinnzahlen"
    

    r = requests.get(url, headers=headers)
    html = r.text
    bs = BeautifulSoup(html, "html.parser")

    entries = bs.findAll("a", {"class": "et_event_ejp_dlnumbers"})
    for e in entries:
        link = e["href"] 
        print(link)
        
        p = urlparse(link)
        print(p.path)
        #print(os.path.realpath("."))
        print(os.getcwd())
        filename = os.path.join(temp_path_download, os.path.basename(p.path))
        print(filename)

        response = requests.get(link)

        os.makedirs(temp_path_download, exist_ok=True)

        zip = open(filename, "wb")
        zip.write(response.content)
        zip.close()

    print(f"File {filename} was downloaded.")

    return filename

def extract_zip(filename):
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(temp_path_download)

def import_eurojackpot_winning_numbers(filename):
    print("Importing Eurojackpot winning numbers...")

    with open(filename, newline = '') as numbers:                                                                                          
    	#numbers_reader = csv.reader(numbers, delimiter="\t")
    	#for row in numbers_reader:
    	#	print(row)
        numbers_list = []
        numbers_reader = csv.DictReader(numbers, delimiter="\t")
        for row in numbers_reader:
            numbers_list.append(dict(row))
                    
        with con.cursor() as cursor:    
            cursor.execute("DELETE FROM {table}".format(table=TABLE_NAME))
            #for dict_item in numbers_list:                
            mylist = [tuple(d.values()) for d in numbers_list]
                        
            for winning_tuple in mylist:
                #values = [[value for value in entry] for entry in dict_item.values()]
                #print(values)

                #execute_values(cursor, sql, argslist)     
                #argument_string = ",".join("('%s', '%s')" % (x, y) for (x, y) in dict_item)                
                #cursor.execute("INSERT INTO {table} (Zahl1, Zahl2, Zahl3, Zahl4, Zahl5) VALUES (%(ZahlA1)s, %(ZahlA2)s, %(ZahlA3)s, %(ZahlA4)s, %(ZahlA5)s)".format(table=TABLE_NAME), dict_item)
                #cursor.execute("INSERT INTO {table} (\"Zahl1\", \"Zahl2\", \"Zahl3\", \"Zahl4\", \"Zahl5\") VALUES (%s, %s, %s, %s, %s)".format(table=TABLE_NAME), dict_item["ZahlA1"], dict_item["ZahlA2"], dict_item["ZahlA3"], dict_item["ZahlA4"], dict_item["ZahlA5"])
    
                #print(winning_tuple)
                ziehungsdatum = ".".join(winning_tuple[0:3])
                new_tuple = (ziehungsdatum,) + winning_tuple[3:]
                #print(new_tuple)
                cursor.execute("INSERT INTO {table}  VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table=TABLE_NAME), new_tuple)
                con.commit()
                
    #with open(filename, "r") as reader:
        #file_content = reader.readlines()
        #print(file_content)   

def determine_eurojackpot_klasse(anz_right_normal_number, anz_right_euro_number):
    with con.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute("SELECT klasse, bezeichnung FROM gewinnklasse_eurojackpot WHERE anzahl_richtige=%s AND anzahl_richtige_eurozahlen=%s", (anz_right_normal_number, anz_right_euro_number))
        gewinnklasse = cursor.fetchone()        

        if gewinnklasse is not None:
            return gewinnklasse
        else:
            return {"klasse": 99, "bezeichnung": "no_win"}

def determine_eurojackpot_wins(year_of_interest: int):
    print("Check winnings...")
    verify_columns = ["zahl1", "zahl2", "zahl3", "zahl4", "zahl5", "eurozahl1", "eurozahl2"]    
    wins = []

    with con.cursor(cursor_factory=RealDictCursor) as tip_cursor:
        tip_cursor.execute("""SELECT l.spielauftragsnummer, CONCAT('TIP', t.tippfeld_nummer) tippfeld_nummer,
                                t.zahl1, t.zahl2, t.zahl3, t.zahl4, t.zahl5,
                                t.eurozahl1, t.eurozahl2
                                FROM lottoschein l
                                JOIN tipp t ON l.id = t.lottoschein_id
                                WHERE aktiv_jn = 'true'""")

        tips = [dict(row) for row in tip_cursor.fetchall()]                                

    with con.cursor(cursor_factory=RealDictCursor) as history_cursor:
        history_cursor.execute("SELECT * FROM gewinnzahlen_eurojackpot ORDER BY ziehungsdatum")
        history = [dict(row) for row in history_cursor.fetchall() if row["ziehungsdatum"].year == year_of_interest]
        
        for t in tips:      # for every tip
            for h in history:   # looking in every history-record     
                keys_to_extract = ["eurozahl1", "eurozahl2"]
                subset_euro_numbers = {key: h[key] for key in keys_to_extract}  # Subset der Euro-Zahlen einer vergangenen Ziehung
                subset_normal_numbers = dict(subset_euro_numbers.items() ^ h.items())
                del subset_normal_numbers["ziehungsdatum"]

                temp_win_numbers = {}
                iNormal_Zahlen = 0
                iEuro_Zahlen = 0

                for k, win_number in h.items():                
                    if k in verify_columns:                        
                        for tip_key, tip_number in t.items():    
                            aHit = False                        
                            if tip_key in subset_normal_numbers.keys() and k in subset_normal_numbers.keys():
                                if tip_number == win_number:
                                    aHit = True
                                    iNormal_Zahlen += 1
                                    fieldname_win_number = "zahl" + str(iNormal_Zahlen)
                            elif tip_key in keys_to_extract and k in keys_to_extract:
                                if tip_number == win_number:
                                    aHit = True
                                    iEuro_Zahlen += 1
                                    fieldname_win_number = "eurozahl" + str(iEuro_Zahlen)

                            if aHit == True:
                                if temp_win_numbers:
                                    temp_win_numbers[fieldname_win_number] = tip_number
                                    temp_win_numbers["anzahl_richtige_normal"] = iNormal_Zahlen
                                    temp_win_numbers["anzahl_richtige_eurozahlen"] = iEuro_Zahlen
                                else:
                                    temp_win_numbers["spielauftragsnummer"] = t["spielauftragsnummer"]
                                    temp_win_numbers["tippfeld_nummer"] = t["tippfeld_nummer"]
                                    temp_win_numbers["ziehungsdatum"] = h["ziehungsdatum"]
                                    temp_win_numbers[fieldname_win_number] = tip_number
                                    
                if temp_win_numbers:
                    #temp_win_numbers["gewinnklasse"] = determine_eurojackpot_klasse(iNormal_Zahlen, iEuro_Zahlen)         
                    gewinnklasse  = determine_eurojackpot_klasse(iNormal_Zahlen, iEuro_Zahlen)  
                    temp_win_numbers["gewinnklasse"] = gewinnklasse["klasse"]
                    temp_win_numbers["gewinnklasse_bez"] = gewinnklasse["bezeichnung"]
                    wins.append(temp_win_numbers)                      

    return wins

def print_eurojackpot_what_happened_if(my_list: list, show_only_win_class_up_to: int = 7):
    my_list = sorted(my_list, key=lambda k: k['gewinnklasse'])      # The sorted() function takes a key= parameter
    for d in my_list:
        if d["gewinnklasse"] <= show_only_win_class_up_to:
            print(f"Gewinnklasse: {d['gewinnklasse']} \tBez: {d['gewinnklasse_bez']} \tZiehungsdatum {d['ziehungsdatum'].strftime('%d.%m.%Y')} \tTippfeld: {d['tippfeld_nummer']} \tSpielauftragsnummer: {d['spielauftragsnummer']}")
        
zip_file = download_eurojackpot_file()
extract_zip(zip_file)
con = connect2psql()
import_eurojackpot_winning_numbers(os.path.join(temp_path_download, filename_eurojackpot))
l = determine_eurojackpot_wins(2022)
print_eurojackpot_what_happened_if(l, 12)