# laeme vajalikud moodulid
import csv
from datetime import datetime

# avame csv faili
with open("input.csv", encoding="utf-8") as csv_file:
    # konverteerime csv faili python nimekirjaks (for tsükli jaoks)
    csv_reader = list(csv.reader(csv_file, delimiter=","))

    cache_dictionary = dict()  # python dictionary (võtme-paaride kogum)

    # käime for tsükli abil csv faili läbi
    for row in csv_reader[1:]:
        # veerus kaksteist (12) asub ehr kood ning kogu andmete uuendamise ahel
        if row[12] in cache_dictionary.keys():
            # kui veerus kaksteist asuv ehr kood asub pythoni dictionary's (võti), siis võetakse
            # selle võtme all asuv csv rida ning võrreldakse selle kuupäeva praeguse for tsüklis
            # kasutatava csv kuupäevaga
            cached_data = cache_dictionary[row[12]]
            cached_time = datetime.strptime(cached_data[11], '%Y/%m/%d %H:%M:%S')
            current_time = datetime.strptime(row[11], '%Y/%m/%d %H:%M:%S')

            # kui for tsüklis asuva csv rea kuupäev on uuem kui võtme-paaride kogumis asuv kuupäev, siis
            # uuendatakse võtme-paaride kogumikus käesolevat csv rida
            if current_time > cached_time:
                cache_dictionary[row[12]] = row
        else:
            # ehr kood lisatakse python dictionarysse võtmena ja tema väärtuseks määratakse hetkel
            # for tsüklis asuv csv rida
            cache_dictionary[row[12]] = row

    # tõlgime python dictionary ümber csv formaadiks
    result = [csv_reader[0]]
    for key in cache_dictionary.keys():
        result.append(cache_dictionary[key])

    # salvestame tulemuse csv failina
    with open("kuupäevade-alusel-sorteeritud.csv", "w", encoding="utf-8") as output_file:
        write = csv.writer(output_file)
        write.writerows(result)
