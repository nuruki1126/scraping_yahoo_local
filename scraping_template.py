import asyncio
import urllib.request
from datetime import datetime
from bs4 import BeautifulSoup
import math
from datetime import datetime
import re
import csv
import os
from tabulate import tabulate # ターみたるに綺麗に表示してくれるやつ
import time
import random

url_genre = [
            "hair",
            "g-nail",
            "relax",
            "esthe"
            ]

table_dic = ["slnDataTbl bdCell bgThNml fgThNml vaThT pCellV10H12 mT20", # htmlのテーブル名がジャンルによって違う場合
             "wFull bdCell bgThNml fgThNml vaThT pCellV10H12 bgWhite mT20",
             "wFull bdCell bgThNml fgThNml vaThT pCellV10H12 bgWhite mT20",
             "wFull bdCell bgThNml fgThNml vaThT pCellV10H12 bgWhite mT20"
             ]

area_dic = ["北海道・東北", "関東", "甲信越・北陸", "東北", "関西", "四国", "中国", "九州・沖縄"]

genre_dic = ["美容室・ヘアサロン", "ネイル・まつ毛サロン", "リラクゼーション", "エステサロン"]

pref_dic = [
            [1,"北海道"], [2, "青森"], [3, "岩手"], [4, "宮城"], [5, "秋田"], [6, "山形"], [7, "福島"],
            [8 ,"茨城"], [9, "栃木"], [10, "群馬"], [11, "埼玉"], [12, "千葉"], [13, "東京"], [14, "神奈川"],
            [15, "新潟"], [16, "富山"], [17, "群馬"], [18, "福井"], [19, "山梨"], [20, "長野"], [21, "岐阜"],
            [22, "静岡"], [23, "愛知"], [24, "三重"], [25, "滋賀"], [26, "京都"], [27, "大阪"], [28, "兵庫"],
            [29, "奈良"], [30, "和歌山"], [31, "鳥取"], [32, "島根"], [33, "岡山"], [34, "広島"], [35, "山口"],
            [36, "徳島"], [37, "香川"], [38, "愛媛"], [39, "高知"], [40, "福岡"], [41, "佐賀"], [42, "長崎"],
            [43, "熊本"], [44, "大分"], [45, "宮崎"], [46, "鹿児島"], [47, "沖縄"]
        ]

pref_num_dic = [
            41, 
            51, 52, 53, 54, 55, 56, 
            15, 16, 17, 13, 14, 11, 12, 
            61, 62, 63, 64, 65, 66, 
            31, 32, 33, 34, 
            21, 22, 23, 24, 25, 26, 
            71, 72, 73, 74, 75, 
            81, 82, 83, 84, 
            91, 92, 93, 94, 95, 96, 97, 98
        ]

orders = ["店名", "電話", "住所", "アクセス", "営業時間", "定休日", "お店のホームページ", "支払い方法"]

class Scraping:
    def __init__(self, urls):
        self.urls = urls

    async def run(self):
        await self.fetch()

    async def fetch(self):
        href_array = []
        repeat_nums = await asyncio.gather(*(self.calc_repeat_num(url.format(1)) for url in self.urls)) # calc_repeat_numから取得したデータをrepeat_numsに追加
        #("All repeat numbers:", repeat_nums) #デバッグ用

        for url, repeat_num  in zip(self.urls, repeat_nums): # それぞれに実行するためのzipとfor
            output_csv((await asyncio.gather(self.get_href(url, repeat_num))), url)


    #各都道府県,各ジャンルの繰り返し回数を計算する関数
    # @param :  self
    #           url
    # @return:  repeat_num (ページネーション数)
    async def calc_repeat_num(self, url):
        pattern = re.compile(r'[\n\s\u3000\,]') # パターンを生成

        request = urllib.request.Request(url)
        html = urllib.request.urlopen(request).read()
        bs = BeautifulSoup(html, "html.parser")

        count_num = pattern.sub('', bs.find(class_ = "numberOfResult").text)
        repeat_num = math.ceil((int(count_num) / 20.0)) #切り上げを行う

        print(f"\033[32mProcess\033[0m: {url}の最大ページ数を取得しています")
        print(f"\033[32mProcess\033[0m: {url}の最大ページ数は{repeat_num}です")

        return repeat_num

    #表示ページのhref(リンク)を取得する関数
    # @param:   self
    #           url
    #           repeat_nums (各ページネーションの数)
    # @return:  detail_datas (詳細情報を格納した配列)
    async def get_href(self, url, repeat_nums):
        pattern = re.compile(r'[\n\s\u3000]') # 正規表現 改行:\n,空白\s,全角空白:\u3000 を除外する
        detail_datas = [] # 詳細情報の配列

        genre_name = ""

        url_pattern = r'https://beauty\.hotpepper\.jp/([^/]+)/([^/]+)/' # ハイフンにも対応できる様に"[\w-]+)"へ変更
        match = re.match(url_pattern, url)

        if match:
            genre_name = match.group(1)
        try:
            genre_index = url_genre.index(genre_name)
        except ValueError:
            genre_index = 0


        if(repeat_nums == 1):
            for page in range(1, 2): 
                now_url = url.format(page) # 現在のページurl
                request = urllib.request.Request(now_url) # urlリクエスト
                html = urllib.request.urlopen(request).read() # htmlの取得
                soup = BeautifulSoup(html, "html.parser") # htmlの解析

                shop_info_array = soup.find_all(class_ ='shopDetailTop shopDetailWithCourseCalendar')

                
                for i in range(0, 20): # 表示したページのhref20件を取得shop_name header_font_size_l font_bold
                    try:
                        shop_name = pattern.sub('', shop_info_array[i].find(class_ = 'shopDetailBottom').find(class_ = 'shopDetailInnerTop').find(class_ = 'shopDetailInnerBottom').find(class_ = 'shopDetailInnerMiddle').find(class_ = 'shopDetailCoreInner cf').find(class_ = 'shopDetailText').find(class_ = 'shopDetailStoreName').find('a').get('href'))

                        info = await asyncio.gather(self.scraping(shop_name))
                        if info[0] is not None:
                            detail_datas.append((info)[0])

                    except IndexError:
                        print(i)
                        pass
                # ランダムな待ち時間だけ処理を停止する
                time.sleep(random.randint(0, 5))
        else:
            for page in range(1, repeat_nums + 1): # 各ページを表現
            #for page in range(1, 2): # デバッグ用
                now_url = url.format(page) # 現在のページurl

                request = urllib.request.Request(now_url) # urlリクエスト
                html = urllib.request.urlopen(request).read() # htmlの取得
                soup = BeautifulSoup(html, "html.parser") # htmlの解析

                if(genre_name == "g-nail" or genre_name == "relax" or genre_name == "esthe"):
                    shop_info_array = soup.find_all(class_ ='slcHead')
                else:
                    shop_info_array = soup.find_all(class_ ='slnName')

                for i in range(0, 20): # 表示したページのhref20件を取得shop_name header_font_size_l font_bold
                    try:
                        shop_href = pattern.sub('', shop_info_array[i].find('a').get('href'))
                        info = await asyncio.gather(self.scraping(shop_href, genre_index))
                        if info[0] is not None:
                            detail_datas.append((info)[0])

                    except IndexError:
                        print(i)
                        pass
                # ランダムな待ち時間だけ処理を停止する
                time.sleep(random.randint(0, 5))

        return detail_datas

    # 詳細ページの情報を取得する関数
    # @param href (詳細ページのリンク)
    #        shop_opening_days (お店の開始時間)　
    #        shop_name (店舗名)
    # @return detail_data (1店舗の詳細データの配列)
    async def scraping(self, href, index):
        pattern = re.compile(r'[\n\s\u3000]|\(大きな地図.*') # パターンを生成    
        phone_number_bool = False

        detail_data = [] # 詳細情報の配列

        detail_url = href
        detail_request = urllib.request.Request(detail_url)
        detail_html = urllib.request.urlopen(detail_request).read()
        detail_soup = BeautifulSoup(detail_html, "html.parser")

        table_rows = detail_soup.find(class_ = table_dic[index]).find_all('tr')
        
        detail_data = []

        for i, order in enumerate(orders): # 元のからデータをヘッダーの数だけ生成
            detail_data.append("ー")

        shop_name = pattern.sub('', detail_soup.find(class_ = 'detailTitle').text)
        detail_data[0] = (shop_name)

        for i, row in enumerate(table_rows):
            th = pattern.sub('',  detail_soup.find(class_ = table_dic[index]).find_all('tr')[i].find(class_ = 'w120').text)
            if(th ==("住所")):
                shop_address = pattern.sub('', row.find_all(['td'])[0].text) 
                detail_data[2] = (shop_address)
            elif(th ==("アクセス・道案内")):
                shop_access = pattern.sub('', row.find_all(['td'])[0].text) 
                detail_data[3] = (shop_access)
            elif(th ==("電話番号")):
                phone_number_href = row.find_all(['td'])[i].find('a').get('href')
                shop_phone_number = await asyncio.gather(self.get_phone_number(phone_number_href)) # 配列で格納されている
                detail_data[1] = (shop_phone_number[0])
            elif(th == ("営業時間")):
                shop_opening_time = pattern.sub('', row.find_all(['td'])[0].text) 
                detail_data[4] = (shop_opening_time)
            elif(th == ("定休日")):
                shop_regular_holiday = pattern.sub('', row.find_all(['td'])[0].text) 
                detail_data[5] = (shop_regular_holiday)
            elif(th == ("お店のホームページ")):
                shop_hp = pattern.sub('', row.find_all(['td'])[0].text) 
                detail_data[6] = (shop_hp)
            elif(th == ("支払い方法")):
                pay = pattern.sub('', row.find_all(['td'])[0].text) 
                detail_data[7] = (pay)

        print(f"\033[32mProcess\033[0m: {detail_data}")
                
        return detail_data
    
    async def get_phone_number(self, href):
        pattern = r'\b0\d{1,4}-\d{1,4}-\d{4}\b'

        detail_url = href
        detail_request = urllib.request.Request(detail_url)
        detail_html = urllib.request.urlopen(detail_request).read()
        detail_soup = BeautifulSoup(detail_html, "html.parser")

        phone_numebr = detail_soup.find(class_ = "fs16 b").text

        return phone_numebr.replace("\xa0", "")
        
        
# CSVファイルを出力する関数
# @param :detail_datas (詳細データの配列)
#        :url (カテゴリ, 地域を判定するためのurl)
def output_csv(detail_datas, url):
    
    url_pattern = r'https://beauty\.hotpepper\.jp/([^/]+)/([^/]+)/' # ハイフンにも対応できる様に"[\w-]+)"へ変更
    match = re.match(url_pattern, url)

    genre_name = ""  # 選択したジャンル 同上

    if match:
        genre_name = match.group(1)
    try:
        genre_index = url_genre.index(genre_name)
    except ValueError:
        genre_index = 0

    # 正規表現で"SA"の後の数字を抽出
    extracted = ""
    match = re.search(r"pre(\d+)", url)
    if match:
        extracted = match.group(1)

    try:
        folder_path =f"hotpepper_beauty/{pref_dic[(int(extracted)-1)][1]}"
        print(folder_path)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"\033[32mProcess\033[0m: フォルダ '{folder_path}' を作成しました。")
    except ValueError:
        print(f"'{folder_path}' was not found in the list.")

    file_name = ""
    file_name = f"{pref_dic[(int(extracted)-1)][1]}_{genre_dic[int(genre_index)]}.csv"
    with open(os.path.join(folder_path, file_name), 'w', newline='',  encoding='shift-jis', errors="ignore") as csvfile:
        writer = csv.writer(csvfile)

        # ヘッダーを書き込む
        writer.writerow(orders)

        print(detail_datas[0][0][1][0].isdigit(), detail_datas[0][0][1][0])

        for detail_data in detail_datas[0]:
            if detail_data[1][0].isdigit():
                # データを書き込む
                writer.writerows([detail_data])
            else :
                pass
                # print("データなしor電話番号なし")

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
    url = "https://beauty.hotpepper.jp/pre{}/PN{}/"
    url_nail = "https://beauty.hotpepper.jp/g-nail/pre{}/PN{}/"
    url_relax = "https://beauty.hotpepper.jp/relax/pre{}/PN{}"
    url_esthe = "https://beauty.hotpepper.jp/esthe/pre{}/PN{}"


    url_array = []

    select_genres_index = []
    select_prefs_index = []

    # 赤色 \033[31mThis is red text.\033[0m
    # 緑色 \033[32mThis is green text.\033[0m
    # 黄色 \033[33mThis is yellow text.\033[0m
    # 青色 \033[34mThis is blue text.\033[0m

    print("Process: ジャンルを選択してください")
    for index, genre in enumerate(genre_dic):
        print(f"{index + 1}",":",f"{genre}")
    print("9 : 全ジャンル選択")

    flag = True

    while True:
        print("\033[34mInput\033[0m: 半角で数値を入力してください (1-4) [コマンド y:エリア選択に進む, d:追加した要素を削除, スペース:複数入力] ")
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
                print(f"Selected Genre: {genre_dic[int(genre_index)]}")

        except ValueError: # 数字以外の入力
            if genre_index == "d": # 空の配列を削除しようとした時の処理
                if len(select_genres_index) != 0: # 配列に値が存在する場合の処理
                    print(f"Delete: {genre_dic[int(select_genres_index[-1])]}を削除しました")
                    select_genres_index.pop()
                    flag = True
                else:
                    print("ジャンルが選択されていません")
                for genre_index in select_genres_index:
                    print(f"Selected Genre: {genre_dic[int(genre_index)]}")
            elif genre_index == "y":
                for genre_index in select_genres_index:
                    print(f"Selected Genre: {genre_dic[int(genre_index)]}")
                break
            else:
                print("Warn: 0 ~ 5の整数を入力してください")

    print("Process: 都道府県を1~47の整数で選択してください")
    print(tabulate([pref_dic[i:i+7] for i in range(0, len(pref_dic), 7)], tablefmt="plain", stralign="center")) # 7件で折り返す

    # 都道府県選択を行うwhile文
    while True:
        #print(tabulate(pref_dic, tablefmt="plain", stralign="center"))
        print("\033[34mInput\033[0m: 都道府県を半角数値で選択してください [コマンド y:CSVファイルを出力, d:追加した要素を削除, スペース:複数入力]]: ")
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
                print(select_genre_index)
                if(int(select_genre_index) == 0):
                    url_array.append(url.format(str(pref_dic[int(select_pref_index)-1][0]).zfill(2), "{}"))
                elif(int(select_genre_index) == 1):
                    url_array.append(url_nail.format(str(pref_dic[int(select_pref_index)-1][0]).zfill(2), "{}"))
                elif(int(select_genre_index) == 2):
                    url_array.append(url_relax.format(str(pref_dic[int(select_pref_index)-1][0]).zfill(2), "{}"))
                elif(int(select_genre_index) == 3):
                    url_array.append(url_esthe.format(str(pref_dic[int(select_pref_index)-1][0]).zfill(2), "{}"))

        print(url_array)

        scraping = Scraping(url_array)

        await scraping.run()
    else:
        print(f"\033[31mWarn\033[0m : 選択されませんでした")

asyncio.run(main())