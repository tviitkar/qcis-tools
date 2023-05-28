# laeme vajalikud python moodulid
import csv
import itertools
import time

import requests

# kolmanda osapoole kirjutatud Pythoni moodul wkb formaadi
# wkt formaati transleerimiseks
# url: https://github.com/thehappycheese/parse_wkb
from wkb import wkb_to_wkt as wkb

# korduvad csv faili veergude indeksid
# vähendab veaohtu ja saan sama väärtust korduvalt uuesti kasutada
COORDINATE_COLUMN_IDX = 8  # hex formaadis koordinaadid asuvad 8ndas veerus

# avame algandmete faili
with open("Algandmed.csv", encoding="utf-8") as csv_file:

    # loendurid (statistika jaoks)
    value_errors = 0
    http_request_error = 0
    building_not_found = 0
    empty_fields = 0
    result = []

    start_time = time.time()  # automaatika stardi aeg

    # avame csv faili, konverteerime pythoni nimekirjaks (for tsükli jaoks)
    csv_reader = list(csv.reader(csv_file, delimiter=","))
    # eemaldame python nimekirjast topelt-kirjed
    csv_reader = list(k for k, _ in itertools.groupby(csv_reader))

    batch_counter = 0  # abimuutuja. selgitus koodi alumises osas, kus seda loendurit kasutatakse

    # käima csv faili for tsükli abil läbi, ignoreerime esimest rida, kuna
    # päistes ei ole meie jaoks vajalikku informatsiooni
    for line in csv_reader[1:]:

        try:
            if line[COORDINATE_COLUMN_IDX]:
                # print(f"Current: {line}")  # debug
                # kasutame kolmanda-osapoole koodi, mis konverteerib hex formaadis koordinaadid (wbx) ümber
                # wtx kujule, et saaksime koordinaate qgis-is kenasti sisse laadida ja kasutada
                parsed_to_wkt, any_remaining_bytes = wkb.wkb_to_wkt(bytearray.fromhex(line[COORDINATE_COLUMN_IDX]))
                line[COORDINATE_COLUMN_IDX] = parsed_to_wkt  # vahetame csv failis wbx väärtuse wtx väärtuse vastu
            else:
                # mõnel real ei pruugi wbx väärtuse väli täidetud olla ning
                # sellisel juhul ei tee kood mitte midagi ja liigub järgmise kirje juurde
                empty_fields += 1
        except ValueError:
            # hex formaadi konverteerija satub aeg-ajalt segadusse ja ei suuda sisendit korrektselt tõlgendada.
            # juurpõhjus on hetkel teadmata ja kategoriseerin selle kolmanda osapoole konverteerija veana.
            # püüame sellest situatsioonist tuleneva veateate kinni ja teeme objekti aadressi alusel päringu
            # ehr-i andmebaasi kasutades selleks nende enda avalikult kättesaadavat APIt.

            value_errors += 1  # statistika jaoks
            # print(f"Value error, getting data for item number {value_errors}")  # debug

            # määrame otsitava aadressi
            payload = {"address": line[17]}

            # sooritame ehr andmebaasi päringu
            response = requests.get(url='https://devkluster.ehr.ee/api/geoinfo/v1/getgeoobjectsbyaddress', params=payload)

            if response.status_code == 200:  # edukas päring
                data = response.json()  # tõlgime http vastuse json formaati

                building_found = False  # abimuutuja

                # ühe aadressi peal võib olla mitu erinevat hoonet, käime need for tsüklis läbi
                for building in data:

                    # proovime leida aadressilt hoone, mille csv failis asuv
                    # ehr kood klapid aadressil asuva hoone koodiga
                    if line[12] == building["properties"]["object_code"]:
                        # csv faili sobiv koordinaadi formaadi näidis:
                        # "POLYGON ((692129.3 6572921.71, 692130.64 6572929.03, 692137.55 6572927.92))"

                        building_coordinates = str()
                        coordinates_list = []

                        # konverteerime api vastuses peituva koordinaatide nimekirja
                        # csv faili jaoks vajalikku formaati (näidis üleval)
                        for point_coordinates in building["geometry"]["coordinates"][0]:
                            coordinates = ""
                            for point in point_coordinates:
                                if not coordinates:
                                    coordinates += str(point)
                                    continue
                                coordinates += f" {point}"
                            coordinates_list.append(coordinates)

                        # salvestame koordinaadid csv faili
                        building_coordinates = f"{building['geometry']['type'].upper()} (({', '.join(coordinates_list)}))"
                        line[COORDINATE_COLUMN_IDX] = building_coordinates

                        building_found = True  # abimuutuja

                if not building_found:
                    # otsitud aadressilt asuvate hoonete hulgast ei leitud ühtegi hoonet, mille ehr kood
                    # vastaks csv failis asuvale koodile
                    building_not_found += 1
                    print(f"Building not found: {line}")

            else:
                # ehr api päring ebaõnnestus, prindime välja veakoodi, põhjuse ja problemaatilise csv rea
                http_request_error += 1
                print(f"HTTP response code: {response.status_code}")
                print(f"HTTP reason: {response.reason}")
                print(f"Row: {line}")

            batch_counter += 1  # abimuutuja

        if batch_counter >= 50:
            # ehr api päringute vahele tuleb jätte mõistlik ootaeg, sest kui need tuimalt
            # teineteise otsa teha, siis ehr api lõpetab vastamise ja ignoreerib kõik edasisi päringuid (turva-
            # reziim). lahendasin probleemi nii, et teen päringuid 50-te gruppide kaupa ning ootan see-järel
            # ühe minuti ja jooksutan järgmised 50 päringut ja nii edasi.
            print("Ootame minutikese, sest muidu Ehitusregistri API saab kurjaks ja ei kõnele enam meiega.")
            time.sleep(60)  # ootame ühte minuti
            batch_counter = 0  # loenduri nullimine

    end_time = time.time()  # ajaloenduri lõpp (statistika)

    # koondväljavõte, kaua skript töötas, millised vead avastatid jms. huvitav informatsioon.
    print(f"Total runtime: {end_time - start_time} seconds")
    print(f"Total rows: {len(csv_reader) - 1}")
    print(f"Value errors: {value_errors}")
    print(f"Building not found: {building_not_found}")
    print(f"Data not found errors: {http_request_error}")
    print(f"Empty fields: {empty_fields}")

    # salvestame csv sisu faili nimega output.csv
    with open("output.csv", "w", encoding="utf-8") as output_file:
        write = csv.writer(output_file)
        write.writerows(csv_reader)

    # skript on jõudnud lõppu
    print("End of script.")