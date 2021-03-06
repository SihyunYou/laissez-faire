# -*- coding: utf-8 -*-
import pandas as pd
import locale
locale.setlocale(locale.LC_ALL, 'en_US')
import datetime
from datetime import datetime
from datetime import timedelta
import time

somme = {}
somme_profit = {}
somme_profit_list = {}
user_date_initial = {}
profitabilite_minute = []
k = 0
commission = 0.15

f = open('profit_2022_7.txt', 'r', encoding='UTF-8')
_lines = f.readlines()
lines = []
i = 0
pro = 0

for p in range(len(_lines)):
    if _lines[p][0] != '#':
        if _lines[p][-1] == '\n':
            lines.append(_lines[p][:-1])
        else:
            lines.append(_lines[p])

for p in range(len(lines)):
    if lines[p][0] == '\t':
        list_donnee = lines[p][1:].split(' ')
        qui = list_donnee[0]
        montant_recu = int(list_donnee[1])

        if qui in somme.keys():
            somme[qui] += montant_recu
        else:
            somme[qui] = montant_recu

        if p + 1 < len(lines) and lines[p + 1][0] != '\t' or p + 1 == len(lines):
            list_rangee = []

            for key in somme:
                if key in somme_profit.keys():
                    t = evaluation * ((somme[key] + somme_profit[key]) / (sum(somme.values()) + sum(somme_profit.values())))
                    somme_profit[key] += t
                    somme_profit_list[key].append(somme_profit[key])
                else:
                    t = evaluation * somme[key] / sum(somme.values())
                    somme_profit[key] = t
                    user_date_initial[key] = date_initial
                    somme_profit_list[key] = [somme_profit[key]]

            for key in somme:
                rangee = []
                rangee.append(key)
                rangee.append(locale.format_string("%d", evaluation * somme[key] / sum(somme.values()), grouping=True))
                rangee.append(locale.format_string("%d", somme[key], grouping=True))
                #rangee.append(locale.format_string("%d", somme_profit[key], grouping=True))
                rangee.append(locale.format_string("%d", somme_profit[key] * (1 - commission), grouping=True))
                rangee.append(locale.format_string("%f", round(somme_profit[key] / somme[key] * 100, 6), grouping=True))
                rangee.append(user_date_initial[key])
                list_rangee.append(rangee)

            print("?????? ?????? : " + locale.format_string("%d", sum(somme.values()), grouping=True) + "???")
            print("?????? ???????????? : " + locale.format_string("%d", sum(somme_profit.values()), grouping=True) + "???")
            pro += profitabilite
            print("??????????????? : " + locale.format_string("%d", sum(somme_profit.values()) * commission, grouping=True) + "???")
            print("???????????? ?????????(?????? ?????????) : " + str(round(pro, 6)) + "%")
            
            print("?????????????????? ???????????? : " + locale.format_string("%d", evaluation, grouping=True) + '???')
            print("????????? ?????? ???????????? : " + locale.format_string("%d", evaluation / diff_second * 3600, grouping=True) + '???')
            print("????????? : " + str(round(profitabilite, 6)) + "%")
            #print("????????? ?????? ????????? : " + locale.format_string("%f", profitabilite / diff_second * 3600, grouping=True) + '%')

            df = pd.DataFrame(list_rangee, columns = ['?????????', '?????? ????????????', '?????? ??????', '????????????(?????? ???)', '?????????(%)', '?????????????????????'])
            print(df.to_markdown()) 
            print('\n')
    else:
        list_donnee = lines[p].split(' ')
        date_initial = list_donnee[0]
        temp_initial = datetime.strptime(date_initial, '%Y.%m.%d.%H:%M')
        date_final = list_donnee[1]
        temp_final = datetime.strptime(date_final, '%Y.%m.%d.%H:%M')
        diff_second = (temp_final - temp_initial).total_seconds()

        event = int(list_donnee[2])
        montant_initial = int(list_donnee[3])
        montant_final = int(list_donnee[4])

        print(date_initial + ' ~ ' + date_final + ' (' + str(temp_final - temp_initial) + ', time_diff=' + str(diff_second) + ')' )
        i += 1
        if event == 1:
            print("????????? " + str(i) + " : ?????????")
        elif event == 2:
            print("????????? " + str(i) + " : ????????? ??????")
        elif event == 3:
            print("????????? " + str(i) + " : ?????? ??????")
        print("????????????(??????/??????) : " + locale.format_string("%d", montant_initial, grouping=True) + '??? ~ ' + locale.format_string("%d", montant_final, grouping=True) + '???')

        evaluation = montant_final - montant_initial
        profitabilite = evaluation / montant_initial * 100

        for k in range(int(diff_second / 60)):
            profitabilite_minute.append(profitabilite)

        if event >= 2:
            for key in somme:
                if key in somme_profit.keys():
                    t = evaluation * ((somme[key] + somme_profit[key]) / (sum(somme.values()) + sum(somme_profit.values())))
                    somme_profit[key] += t
                    somme_profit_list[key].append(somme_profit[key])
                else:
                    t = evaluation * somme[key] / sum(somme.values())
                    somme_profit[key] = t
                    somme_profit_list[key] = [somme_profit[key]]

            list_rangee = []
            for key in somme:
                rangee = []
                rangee.append(key)
                rangee.append(locale.format_string("%d", evaluation * somme[key] / sum(somme.values()), grouping=True))
                rangee.append(locale.format_string("%d", somme[key], grouping=True))
                #rangee.append(locale.format_string("%d", somme_profit[key], grouping=True))
                rangee.append(locale.format_string("%d", somme_profit[key] * (1 - commission), grouping=True))
                rangee.append(locale.format_string("%f", round(somme_profit[key] / somme[key] * 100, 6), grouping=True))
                rangee.append(user_date_initial[key])
                list_rangee.append(rangee)

            print("?????? ?????? : " + locale.format_string("%d", sum(somme.values()), grouping=True) + "???")
            print("?????? ???????????? : " + locale.format_string("%d", sum(somme_profit.values()), grouping=True) + "???")
            pro += profitabilite
            print("??????????????? : " + locale.format_string("%d", sum(somme_profit.values()) * commission, grouping=True) + "???")
            print("???????????? ?????????(?????? ?????????) : " + str(round(pro, 6)) + "%")

            print("?????????????????? ???????????? : " + locale.format_string("%d", evaluation, grouping=True) + '???')
            print("????????? ?????? ???????????? : " + locale.format_string("%d", evaluation / diff_second * 3600, grouping=True) + '???')
            print("????????? : " + str(round(profitabilite, 6)) + "%")
            #print("????????? ?????? ????????? : " + locale.format_string("%f", profitabilite / diff_second * 3600, grouping=True) + '%')

            df2 = pd.DataFrame(list_rangee, columns = ['?????????', '?????? ????????????', '?????? ??????', '????????????(?????? ???)', '?????????(%)', '?????????????????????'])
            print(df2.to_markdown()) 
            print('\n')

evaluation_jour = []
for p in range(int(len(profitabilite_minute) / 1440)):
    somme = 0
    for q in range(1440):
        somme += profitabilite_minute[p * 1440 + q]
    evaluation_jour.append(somme / 1440)

#print(len(evaluation_jour))
#print(evaluation_jour)
