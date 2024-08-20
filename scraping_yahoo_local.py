import urllib.request
import ssl
import certifi
from bs4 import BeautifulSoup
import asyncio
import math
from datetime import datetime
import re
import csv
import os
from tabulate import tabulate # ターみたるに綺麗に表示してくれるやつ
import math
import copy

genre_dic = [
             ["01", "グルメ"],
             ["02", "ショッピング"],
             ["03", "レジャー、エンタメ"], 
             ["04", "暮らし、生活"]
            ]

detail_genres_dic = [
            [
                "0101001",
                "0101002",
                "0101003",
                "0101004",
                "0101005",
                "0101006",
                "0101007",
                "0101008",
                "0101009",
                "0101010",
                "0101011",
                "0101012",
                "0101013",
                "0101014",
                "0101015",
                "0101016",
                "0101017",
                "0101018",
                "0101019",
                "0101020",
                "0101021",
                "0101022",
                "0101023",
                "0101024",
                "0101025",
                "0101026",
                "0101027",
                "0101028",
                "0101029",
                "0101030",
                "0101031",
                "0101032",
                "0101033",
                "0101034",
                "0101035",
                "0101036",
                "0101037",
                "0101038",
                "0101039",
                "0101040",
                "0101041",
                "0101042",
                "0101043",
                "0101044",
                "0101045",
                "0101046",
                "0101047",
                "0101048",
                "0101049",
                "0101050",
                "0101051",
                "0101052",
                "0101053",
                "0102001",
                "0102002",
                "0102003",
                "0102004",
                "0102005",
                "0102006",
                "0102007",
                "0102008",
                "0102009",
                "0102010",
                "0102011",
                "0102012",
                "0102013",
                "0102014",
                "0102015",
                "0102016",
                "0102017",
                "0102018",
                "0102019",
                "0102020",
                "0102021",
                "0102022",
                "0103001",
                "0104001",
                "0104002",
                "0104003",
                "0104004",
                "0104005",
                "0104006",
                "0104007",
                "0104008",
                "0104009",
                "0105001",
                "0105002",
                "0105003",
                "0105004",
                "0105005",
                "0105006",
                "0105007",
                "0105008",
                "0105009",
                "0105010",
                "0105011",
                "0105012",
                "0105013",
                "0105014",
                "0106001",
                "0106002",
                "0107001",
                "0107002",
                "0107003",
                "0107004",
                "0107005",
                "0108001",
                "0108002",
                "0108003",
                "0109001",
                "0110001",
                "0110002",
                "0110003",
                "0110004",
                "0110005",
                "0110006",
                "0111001",
                "0112001",
                "0112002",
                "0113001",
                "0113002",
                "0113003",
                "0114001",
                "0114002",
                "0114003",
                "0115001",
                "0115002",
                "0115003",
                "0115004",
                "0115005",
                "0115006",
                "0115007",
                "0115008",
                "0115009",
                "0115010",
                "0115011",
                "0115012",
                "0115013",
                "0115014",
                "0116001",
                "0116002",
                "0116003",
                "0116004",
                "0117001",
                "0117002",
                "0117003",
                "0117004",
                "0117005",
                "0118001",
                "0118002",
                "0118003",
                "0118004",
                "0119001",
                "0119002",
                "0119003",
                "0119004",
                "0119005",
                "0119006",
                "0119007",
                "0119008",
                "0119009",
                "0119010",
                "0119011",
                "0120001",
                "0120002",
                "0120003",
                "0120004",
                "0121001",
                "0121002",
                "0122001",
                "0123001",
                "0123002",
                "0123003",
                "0124001",
                "0124002",
                "0125001",
                "0125002",
                "0126001",
                "0126002",
                "0127001",
                "0128001"
            ]
            ,
            [
                "0202001",
                "0202002",
                "0202003",
                "0202004",
                "0202005",
                "0203001",
                "0203002",
                "0203003",
                "0203004",
                "0204001",
                "0204002",
                "0204003",
                "0204004",
                "0204005",
                "0205001",
                "0205002",
                "0206001",
                "0206002",
                "0206003",
                "0207001",
                "0207002",
                "0207003",
                "0207004",
                "0207005",
                "0207006",
                "0207007",
                "0207008",
                "0207009",
                "0207010",
                "0208001",
                "0208002",
                "0208003",
                "0208004",
                "0208005",
                "0208006",
                "0208007",
                "0208008",
                "0209001",
                "0209002",
                "0209003",
                "0209004",
                "0209005",
                "0209006",
                "0209007",
                "0209008",
                "0209009",
                "0209010",
                "0209011",
                "0209012",
                "0209013",
                "0209014",
                "0209015",
                "0209016",
                "0209017",
                "0209018",
                "0210001",
                "0210002",
                "0210003",
                "0210004",
                "0210005",
                "0210006",
                "0210007",
                "0210008",
                "0210009",
                "0210010",
                "0210011",
                "0210012",
                "0210013",
                "0211001",
            ]
            ,
            [
                "0301001",
                "0301002",
                "0301003",
                "0301004",
                "0301005",
                "0301006",
                "0301007",
                "0301008",
                "0301009",
                "0301010",
                "0301011",
                "0301012",
                "0301013",
                "0302002",
                "0302003",
                "0303001",
                "0303002",
                "0303003",
                "0303004",
                "0303005",
                "0303006",
                "0303007",
                "0303008",
                "0303009",
                "0303010",
                "0303011",
                "0303012",
                "0303013",
                "0304001",
                "0304002",
                "0304003",
                "0304004",
                "0304005",
                "0304006",
                "0304007",
                "0304008",
                "0304009",
                "0304010",
                "0305001",
                "0305002",
                "0305003",
                "0305004",
                "0305005",
                "0305006",
                "0305007",
                "0306001",
                "0306002",
                "0306003",
                "0306004",
                "0306005",
                "0306006",
                "0306007",
                "0306008",
                "0306009",
                "0306010",
                "0307001",
                "0307002",
                "0307003",
                "0307004",
                "0307005",
                "0307006",
                "0308001",
            ]
            ,
            [
                "0401001",
                "0401002",
                "0401003",
                "0401004",
                "0401005",
                "0401006",
                "0401007",
                "0401008",
                "0401009",
                "0401010",
                "0401011",
                "0401012",
                "0401013",
                "0401014",
                "0401015",
                "0401016",
                "0401017",
                "0401018",
                "0401019",
                "0401020",
                "0401021",
                "0401022",
                "0401023",
                "0401024",
                "0401025",
                "0401026",
                "0401027",
                "0401028",
                "0401029",
                "0401030",
                "0401031",
                "0401032",
                "0401033",
                "0401034",
                "0401035",
                "0401036",
                "0401037",
                "0401038",
                "0401039",
                "0401040",
                "0401041",
                "0401042",
                "0401043",
                "0401044",
                "0401045",
                "0401046",
                "0401047",
                "0401048",
                "0401049",
                "0402001",
                "0403001",
                "0403002",
                "0403003",
                "0403004",
                "0403005",
                "0403006",
                "0404001",
                "0404002",
                "0404003",
                "0404004",
                "0404005",
                "0404006",
                "0405001",
                "0405002",
                "0405003",
                "0405004",
                "0405005",
                "0405006",
                "0405007",
                "0405008",
                "0405009",
                "0405010",
                "0405011",
                "0405012",
                "0405013",
                "0405015",
                "0405016",
                "0405017",
                "0405018",
                "0405019",
                "0405020",
                "0405021",
                "0405022",
                "0405023",
                "0405024",
                "0405025",
                "0405026",
                "0405027",
                "0405028",
                "0405029",
                "0405030",
                "0405031",
                "0406001",
                "0406002",
                "0406003",
                "0406004",
                "0406005",
                "0406006",
                "0406007",
                "0406008",
                "0406009",
                "0406010",
                "0406011",
                "0407001",
                "0407002",
                "0407003",
                "0407004",
                "0407005",
                "0407006",
                "0407007",
                "0407008",
                "0407009",
                "0407010",
                "0408001",
                "0408002",
                "0408003",
                "0408004",
                "0408005",
                "0408006",
                "0408007",
                "0408008",
                "0408009",
                "0408010",
                "0408011",
                "0409001",
                "0409002",
                "0409003",
                "0409004",
                "0409005",
                "0409006",
                "0409007",
                "0409008",
                "0409009",
                "0409010",
                "0410001",
                "0410002",
                "0410003",
                "0410004",
                "0410005",
                "0410006",
                "0410007",
                "0411001",
                "0411002",
                "0412001",
                "0412002",
                "0412003",
                "0412004",
                "0412005",
                "0412006",
                "0412007",
                "0412008",
                "0412009",
                "0412010",
                "0412011",
                "0412012",
                "0412013",
                "0412014",
                "0412015",
                "0412016",
                "0412017",
                "0412018",
                "0412019",
                "0412020",
                "0412021",
                "0412022",
                "0412023",
                "0412024",
                "0412025",
                "0412026",
                "0412027",
                "0412028",
                "0412029",
                "0412030",
                "0412031",
                "0412032",
                "0413001",
                "0413002",
                "0413003",
                "0413004",
                "0413005",
                "0413006",
                "0413007",
                "0413008",
                "0413009",
                "0413010",
                "0413011",
                "0413012",
                "0413013",
                "0413014",
                "0413015",
                "0413016",
                "0413017",
                "0414001",
                "0414002",
                "0414003",
                "0415001",
                "0415002",
                "0415003",
                "0415004",
                "0415005",
                "0416001",
                "0416002",
                "0416003",
                "0416004",
                "0416005",
                "0416006",
                "0416007",
                "0416008",
                "0417001",
                "0417002",
                "0417003",
                "0417004",
                "0417005",
                "0417006",
                "0417007",
                "0418001",
                "0418002",
                "0418003",
                "0418004",
                "0418005",
                "0418006",
                "0419001",
                "0419002",
                "0419003",
                "0419004",
                "0419005",
                "0419006",
                "0419007",
                "0419008",
                "0419009",
                "0419010",
                "0419011",
                "0419012",
                "0419013",
                "0419014",
                "0420001",
                "0420002",
                "0420003",
                "0420004",
                "0420005",
                "0420006",
                "0420007",
                "0420008",
                "0420009",
                "0420010",
                "0420011",
                "0420012",
                "0420013",
                "0421001",
                "0421002",
                "0421003",
                "0421004",
                "0422001",
                "0422002",
                "0422003",
                "0422004",
                "0423001",
                "0423002",
                "0423003",
                "0423004",
                "0423005",
                "0423006",
                "0423007",
                "0423008",
                "0423009",
                "0424001",
                "0424002",
                "0424003",
                "0424004",
                "0425001",
                "0425002",
            ]

]

pref_dic = [
            ["01","北海道"],["02", "青森"],   ["03", "岩手"], ["04", "宮城"],   ["05", "秋田"], ["06", "山形"], ["07", "福島"],
            ["08" ,"茨城"], ["09", "栃木"],   ["10", "群馬"], ["11", "埼玉"],   ["12", "千葉"], ["13", "東京"], ["14", "神奈川"],
            ["15", "新潟"], ["16", "富山"],   ["17", "群馬"], ["18", "福井"],   ["19", "山梨"], ["20", "長野"], ["21", "岐阜"],
            ["22", "静岡"], ["23", "愛知"],   ["24", "三重"], ["25", "滋賀"],   ["26", "京都"], ["27", "大阪"], ["28", "兵庫"],
            ["29", "奈良"], ["30", "和歌山"], ["31", "鳥取"], ["32", "島根"],   ["33", "岡山"], ["34", "広島"], ["35", "山口"],
            ["36", "徳島"], ["37", "香川"],   ["38", "愛媛"], ["39", "高知"],   ["40", "福岡"], ["41", "佐賀"], ["42", "長崎"],
            ["43", "熊本"], ["44", "大分"],   ["45", "宮崎"], ["46", "鹿児島"], ["47", "沖縄"]
        ]

orders = ["店名", "電話", "住所", "アクセス", "営業時間", "定休日", "お店のホームページ", "支払い方法"]

class Scraping:
    def __init__(self, urls):
        self.urls = urls

    async def run(self):
        await self.fetch()

    async def fetch(self):
        href_array = []
        repeat_nums = await asyncio.gather(*(self.calc_repeat_num(url) for url in self.urls)) # calc_repeat_numから取得したデータをrepeat_numsに追加

        #("All repeat numbers:", repeat_nums) #デバッグ用
        detail_data_array = []
        url = ""

        for url, repeat_num  in zip(self.urls, repeat_nums): # それぞれに実行するためのzipとfor
            #detail_data.append((await asyncio.gather(self.scraping(url, repeat_num)))[0])
           
            #detail_data_array.extend((await asyncio.gather(self.scraping(url, repeat_num)))[0])
        
            output_csv([(await asyncio.gather(self.scraping(url, repeat_num)))[0]], url)


    #各都道府県,各ジャンルの繰り返し回数を計算する関数
    # @param :  self
    #           url
    # @return:  repeat_num (ページネーション数)
    async def calc_repeat_num( self, url ):
        
        url = url.format(1)
        context = ssl.create_default_context( cafile=certifi.where() )
        try:
            detail_request = urllib.request.Request( url )
            detail_html = urllib.request.urlopen( detail_request, context=context ).read()
            detail_soup = BeautifulSoup( detail_html, "lxml-xml" )

            total = int( detail_soup.find( "Total" ).text )

            repeat_num = 0
            if( total > 100 ):
                repeat_num = math.ceil( total / 100 )  
        except Exception as e:
            print(f"Error occurred: {e}")

        print( "ページ数取得中...", repeat_num )
        return repeat_num

    # 詳細ページの情報を取得する関数
    # @param href (詳細ページのリンク)
    #        shop_opening_days (お店の開始時間)　
    #        shop_name (店舗名)
    # @return detail_data (1店舗の詳細データの配列)
    async def scraping(self, url, repeat_num):
        context = ssl.create_default_context( cafile=certifi.where() )
        detail_data_array = []

        try:
            if(repeat_num == 0):
                detail_request = urllib.request.Request( url.format(1) )
                detail_html = urllib.request.urlopen( detail_request, context=context ).read()
                detail_soup = BeautifulSoup( detail_html, "lxml-xml" )

                features =  detail_soup.find_all( "Feature" )

                for feature in features:
                    detail_data = []
                    try: 
                        detail_data.append( feature.find( "Name" ).text )
                        detail_data.append( feature.find( "Address" ).text )
                        detail_data.append( feature.find( "Tel1" ).text )
                        detail_data.append( feature.find( "Genre" ).find( "Name" ).text )

                        detail_data_array.append(detail_data)
                    except Exception as e:
                        pass
            else:
                for num in range( 0, repeat_num ):
                    i = 100
                    if ( num == 0 ): # 最初のページ100県件取得
                        detail_request = urllib.request.Request( url.format( 1 ) )
                    else:   # 最初のページ以降に101を加算してデータを取得するための IF 文
                        detail_request = urllib.request.Request( url.format( num * i + 1 ) )
                    
                    detail_html = urllib.request.urlopen( detail_request, context=context ).read()
                    detail_soup = BeautifulSoup( detail_html, "lxml-xml" )

                    features =  detail_soup.find_all( "Feature" )

                    for feature in features:
                        detail_data = []
                        try: 
                            detail_data.append( feature.find( "Name" ).text )
                            detail_data.append( feature.find( "Address" ).text )
                            detail_data.append( feature.find( "Tel1" ).text )
                            detail_data.append( feature.find( "Genre" ).find( "Name" ).text )

                            detail_data_array.append(detail_data)
                        except Exception as e:
                            pass

        except Exception as e:
            print(f"Error occurred: {e}")
        
        return detail_data_array
    
   
        
# CSVファイルを出力する関数
# @param :detail_datas (詳細データの配列)
#        :url (カテゴリ, 地域を判定するためのurl)
def output_csv(detail_datas, url):
    gc_pattern = r"gc=([^&]*)"  # ジャンルのindex
    ac_pattern = r"ac=([^&]*)" # 都道府県のindex
    
    gc_match = re.search( gc_pattern, url )
    ac_match = re.search( ac_pattern, url )

    print( url )

    # 一致するデータを取得
    ac_value = ac_match.group(1)
    gc_value = gc_match.group(1)
    
    for i in range(0, len(genre_dic)):
        if ( genre_dic[i][0] == gc_value[:2] ):
            genre_name = genre_dic[i][1]

    for i in range(0, 47):
        if ( pref_dic[i][0] == ac_value ):
            pref_name = pref_dic[i][1]
    try:
        folder_path =f"yahoo_local/{pref_name}"
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"\033[32mProcess\033[0m: フォルダ '{folder_path}' を作成しました。")
    except ValueError:
        print(f"'{folder_path}' was not found in the list.")

    file_name = ""
    file_name = f"{pref_name}_{genre_name}.csv"
    with open(os.path.join(folder_path, file_name), 'w', newline='',  encoding='shift-jis', errors="ignore") as csvfile:
        writer = csv.writer(csvfile)

        # ヘッダーを書き込む
        writer.writerow(orders)

        for detail_data in detail_datas[0]:
                # データを書き込む
            writer.writerows([detail_data])

    print(f"\033[32mProcess\033[0m: {file_name}を作成しました")

# CSVに書き込む配列を整頓する関数
#
# @param :lst (配列)
#        :idnex (order[i]のindex)
#        :value (配列に追加するデータ)
def insert_with_blank(lst, index, value):
    # インデックスがリストの範囲内にある場合
    if 0 <= index < len(lst):
        lst[index] = value
    # インデックスがリストの範囲外である場合
    elif index >= len(lst):
        # 指定されたインデックスまでリストの末尾に空白を追加
        lst.extend([None] * (index - len(lst) + 1))
        lst[index] = value
    else:
        raise IndexError("Index out of range")

async def main():
    url = "https://map.yahooapis.jp/search/local/V1/localSearch?appid=dj00aiZpPUJ4VmJyU1FKOUxRNSZzPWNvbnN1bWVyc2VjcmV0Jng9ZTI-&ac={}&gc={}&start={}&results=100"


    url_array = []

    select_genres_index = []
    select_prefs_index = []

    # 赤色 \033[31mThis is red text.\033[0m
    # 緑色 \033[32mThis is green text.\033[0m
    # 黄色 \033[33mThis is yellow text.\033[0m
    # 青色 \033[34mThis is blue text.\033[0m

    print("Process: ジャンルを選択してください")
    for index, genre in enumerate(genre_dic):
        print(f"{index + 1}",":",f"{genre[1]}")
    print("9 : 全ジャンル選択")

    flag = True

    while True:
        print("\033[34mInput\033[0m: 半角で数値を入力してください (1-4) [コマンド y:エリア選択に進む, d:追加した要素を削除] ")
        i = input("\033[34mEnter\033[0m: ")
        array_genre_index = i.split()
        # print(array_genre_index)
        try:
            for genre_index in array_genre_index:
                genre_index = str(int(genre_index) - 1)
                if 0 <= int(genre_index) <= 3 and genre_index not in select_genres_index: # 入力番号が0~5, まだ入力されていない番号
                    select_genres_index.append(genre_index)
                elif int(genre_index) == int(8): # 全ジャンルの追加
                    for i in range(0, 4):
                        if str(i) not in select_genres_index and flag: # select_genres_indexに含まれていない∧flagがTrue
                            select_genres_index.append(str(i))
                        elif flag == False:
                            print(f"Delete Error: 全ジャンル追加済みです")
                            break
                    flag = False
                elif genre_index in select_genres_index:
                    print(f"Input Error: {genre_dic[int(genre_index)]}はすでに追加済みです")
                else:
                    print(f"Input Error: 入力された[{int(genre_index)+1}]は存在しません")

            for  genre_index in select_genres_index:
                print(f"Selected Genre: {genre_dic[int(genre_index)][1]}")

        except ValueError: # 数字以外の入力
            if genre_index == "d": # 空の配列を削除しようとした時の処理
                if len(select_genres_index) != 0: # 配列に値が存在する場合の処理
                    print(f"Delete: {genre_dic[int(select_genres_index[-1])]}を削除しました")
                    select_genres_index.pop()
                    flag = True
                else:
                    print("ジャンルが選択されていません")
                for genre_index in select_genres_index:
                    print(f"Selected Genre: {genre_dic[int(genre_index)][1]}")
            elif genre_index == "y":
                for genre_index in select_genres_index:
                    print(f"Selected Genre: {genre_dic[int(genre_index)][1]}")
                break
            else:
                print("Warn: 0 ~ 5の整数を入力してください")

    print("Process: 都道府県を1~47の整数で選択してください")
    print(tabulate([pref_dic[i:i+7] for i in range(0, len(pref_dic), 7)], tablefmt="plain", stralign="center")) # 7件で折り返す

    # 都道府県選択を行うwhile文
    while True:
        #print(tabulate(pref_dic, tablefmt="plain", stralign="center"))
        print("\033[34mInput\033[0m: 都道府県を半角数値で選択してください [コマンド y:CSVファイルを出力, d:追加した要素を削除]: ")
        input_pref_index = input("\033[34mEnter\033[0m: ")
        array_pref_index = input_pref_index.split()
        try:
            for pref_index in array_pref_index:
                if 1 <= int(pref_index) <= 47 and str(input_pref_index) not in select_prefs_index:
                    select_prefs_index.append(pref_index)
                else:
                    print(f"\033[31mWarn\033[0m :入力された番号[{pref_index}]は都道府県リストに存在しないか,既に入力されています.")

            for select_pref_index in select_prefs_index:
                print(f"\033[32mSelected Pref\033[0m: {pref_dic[int(select_pref_index)-1][1]}")

        except ValueError:
            if pref_index == "d":
                if len(select_prefs_index) != 0:
                    print(f"\033[33mDelete\033[0m: {pref_dic[int(select_prefs_index[-1])][1]}を削除しました")
                    select_prefs_index.pop()
                else:
                    print("\033[33mDelete Error\033[0m: 都道府県が選択されていません")
            elif pref_index == "y":
                for select_pref_index in select_prefs_index:
                    print(f"\033[32mSelected Pref\033[0m: {pref_dic[int(select_pref_index)-1][1]}")
                break
            else:
                print("\033[33mInput Error\033[0m: 0 ~ 5 の数字を入力してください。")

            for select_pref_index in select_prefs_index:
                print(f"\033[32mSelected Pref\033[0m: {pref_dic[int(select_pref_index)-1][1]}")
                
    if(len(select_prefs_index) != 0):
        for select_genre_index in select_genres_index:
            for select_pref_index in select_prefs_index:
                for detail_genre in detail_genres_dic[int(select_genre_index)]:
                    url_array.append(url.format(str(pref_dic[int(select_pref_index)-1][0]).zfill(2), detail_genre, {}))
                    print( "URL生成中", (url.format(str(pref_dic[int(select_pref_index)-1][0]).zfill(2), detail_genre, {})) )

        scraping = Scraping(url_array)

        await scraping.run()
    else:
        print(f"\033[31mWarn\033[0m : 選択されませんでした")

asyncio.run(main())