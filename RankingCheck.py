# -*- coding: utf-8 -*-

import sys
import os
import pprint
import traceback
import sqlalchemy
import requests
import time
from sqlalchemy.orm import scoped_session, sessionmaker
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy.sql.expression import null
from RankingModels import Base, TSearchM, TSearch, TRanking, TDoc

def __db_upsert(session: scoped_session,soup: BeautifulSoup, t_search: TSearch, ranking: int, max_ranking: int,search_time: datetime, my_url: str, wordjoin: str) -> int:
    """DB登録更新処理

    得られた検索結果をスクレイピングし検索結果をDBに登録する処理をおこなう

    Parameters
    ----------
    session : scoped_session
        データベース接続のセッション
    soup : BeautifulSoup
        検索結果読み込み済みのBeautifulSoupのオブジェクト
    t_search : TSearch
        検索テーブル
    ranking : int
        何位のランキングまで処理完了しているか
    max_ranking : int
        何位までランキングを検索するか
    search_time : datetime
        検索を開始した日時
    my_url : str
        自サイトのURL

    Returns
    -------
    ranking : int
        処理完了したランキング順位
    """

    output_dir = search_time.strftime('%Y-%m-%d') + os.sep + search_time.strftime('%H%M%S')
    #search_time_string = search_time.strftime('%Y%m%d_%H%M%S')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open("{}{}{}_{}.html".format(output_dir,os.sep,wordjoin,str(ranking)), 'w', encoding='UTF-8') as f:
        f.write(soup.prettify())

    divs = soup.select("[class='ZINbbc xpd O9g5cc uUPGi']")
    for div_b in divs:
        if ( div_b.div.a is not None ) and (div_b.div.h3 is not None):
            ranking = ranking + 1
            link_text=div_b.div.a.get("href")
            link_text=link_text.replace('/url?q=','').split('&')[0]
            doc_title=div_b.div.h3.div.get_text().strip()

            t_doc = TDoc()
            t_doc.link_url = link_text
            t_doc.title = doc_title
            if len(my_url) > 0 and my_url in link_text:
                t_doc.mypage_flg = True
            else:
                t_doc.mypage_flg = False
            t_doc = TDoc().upsert(t_doc, session)

            t_ranking = TRanking()
            t_ranking.search_id = t_search.id
            t_ranking.ranking = ranking
            t_ranking.doc_id = t_doc.id
            has_ranking = TRanking().hasRanking(t_ranking, session)
            if has_ranking == True:
                # 二重にランキング計上されているためインサートせずrankingから1を引いておく
                ranking = ranking - 1
            else:
                #t_ranking = TRanking().upsert(t_ranking, session)
                t_ranking = TRanking().insert(t_ranking, session)
            if ranking == max_ranking:
                return max_ranking
    return ranking

def __search_next(session: scoped_session,t_search: TSearch, link_text: str, ranking: int, max_ranking: int,search_time: datetime, my_url: str, wordjoin: str) -> int:
    """検索開始

    検索結果の2ページ以降の処理とおこなう。次項がある場合は再帰的に自身を呼び出す。

    Parameters
    ----------
    session : scoped_session
        データベース接続のセッション
    t_search : TSearch
        検索テーブル
    link_text : str
        検索結果の次頁へのリンクテキスト
    ranking : int
        何位のランキングまで処理完了しているか
    max_ranking : int
        何位までランキングを検索するか
    search_time : datetime
        検索を開始した日時
    my_url : str
        自サイトのURL

    Returns
    -------
    ranking : int
        処理完了したランキング順位
    """

    google_url = "https://www.google.co.jp{}".format(link_text)
    
    r = requests.get(google_url)
    soup = BeautifulSoup(r.text, 'lxml') #要素を抽出

    ret_ranking = __db_upsert(session, soup, t_search, ranking, max_ranking, search_time, my_url, wordjoin)
    if ret_ranking >= max_ranking:
        return ret_ranking

    a = soup.select_one("a[aria-label='次のページ']")
    if a is None:
        return ret_ranking

    link_text=a.get("href")
    time.sleep(1)
    return __search_next(session, t_search, link_text, ret_ranking, max_ranking, search_time, my_url, wordjoin)


def __search_start(session: scoped_session,t_search: TSearch, keywords: list[str], max_ranking: int,search_time: datetime, my_url: str) -> int:
    """検索開始

    検索結果の最初の1ページの処理をおこないDB登録処理を呼び出す

    Parameters
    ----------
    session : scoped_session
        データベース接続のセッション
    t_search : TSearch
        検索テーブル
    keywords : list[str]
        検索キーワード
    max_ranking : int
        何位までランキングを検索するか
    search_time : datetime
        検索を開始した日時
    my_url : str
        自サイトのURL

    Returns
    -------
    ranking : int
        処理完了したランキング順位
    """

    google_url = "https://www.google.co.jp/search"

    search_word=""
    for keyword in keywords:
        #search_word = "{} \"{}\"".format(search_word,keyword)
        search_word = search_word + " " + keyword
    
    wordjoin = "_".join(keywords)

    r = requests.get(google_url,params={'q': search_word})
    soup = BeautifulSoup(r.text, 'lxml') #要素を抽出
    ranking=0
    ret_ranking = __db_upsert(session, soup, t_search, ranking, max_ranking, search_time, my_url, wordjoin)
    if ret_ranking >= max_ranking:
        return ret_ranking

    a = soup.select_one("a[aria-label='次のページ']")
    if a is None:
        return ret_ranking
    link_text=a.get("href")
    time.sleep(1)
    return __search_next(session, t_search, link_text, ret_ranking, max_ranking, search_time, my_url, wordjoin)

def search(keywords: list[str], dbfile: str, url: str, max_ranking: int, drop_flg: bool):
    """検索処理

    検索処理に必要な前処理をおこない検索開始を呼び出す

    Parameters
    ----------
    keywords : list[str]
        検索キーワードの配列
    dbfile : str
        データベースファイル名
    max_ranking : int
        何位までランキングを検索するか
    drop_flg : bool
        TrueのときテーブルをいったんDROPして作成しなおす
    """

    connect_string = "sqlite:///{}".format(dbfile)
    engine = sqlalchemy.create_engine(connect_string, echo=False) # SQLとデータを出力したい場合はecho=Trueにする

    try:
        session = scoped_session(
                    sessionmaker(
                        autocommit = False,
                        autoflush = True,
                        bind = engine))

        Base.query = session.query_property()

        if drop_flg:
            Base.metadata.drop_all(engine)

        Base.metadata.create_all(engine)

        if len(keywords) > 0 and max_ranking > 0:
            tab_keywords = "\t".join(keywords)
            t_search_m = TSearchM()
            t_search_m.keywords=tab_keywords
            t_search_m = TSearchM().upsert(t_search_m,session)

            t_search = TSearch()
            dttime = datetime.now()
            t_search.search_m_id=t_search_m.id
            t_search.search_datetime = dttime
            t_search = TSearch().upsert(t_search,session)

            __search_start(session, t_search, keywords,max_ranking, dttime ,url)

    except Exception:
        raise
    else:
        session.close()
    finally:
        engine.dispose()

def main(argv):
    """メイン処理

    コマンドラインからの引数を受取り変数にセットし検索処理を呼び出す

    Parameters
    ----------
    argv : list[str]
        コマンドラインから入力された文字の配列
    """
    skip = False
    dbfile="ranking.sqlite3"
    url=""
    keyword = []
    max_ranking = 20
    drop_flg = False
    # 引数処理開始
    try:
        for i,arg in enumerate(argv):
            if skip == False and i > 0:
                if arg == '-o':
                    dbfile = argv[i+1]
                    skip = True
                elif arg == '-u':
                    url = argv[i+1]
                    skip = True
                elif arg == '-m':
                    try:
                        max_ranking = int(argv[i+1])
                        if max_ranking <= 0:
                            raise ValueError()
                    except ValueError as _:
                        (exc_type, exc_value, exc_traceback) = sys.exc_info()
                        t = traceback.format_exception(exc_type, exc_value, exc_traceback)
                        t.insert(0,"[ERROR]:mオプションの値は正の整数を指定してください")
                        pprint.pprint(t, width=120,stream=sys.stderr)
                        sys.exit(1)
                    skip = True
                elif arg == '--drop':
                    drop_flg = True
                else:
                    keyword.append(arg)
            else:
                skip = False
        if 0 == len(keyword) and drop_flg == False:
            errlist=[]
            errlist.append("[ERROR]:引数の形がちがいます")
            errlist.append("py RankingCheck.py [--drop] [-m 最大ランキング数] [-u URL] [-o DBファイル名] キーワード1 [キーワード2] [キーワード3] …")
            pprint.pprint(errlist, width=120,stream=sys.stderr)
            sys.exit(1)
    except IndexError as e:
        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        t = traceback.format_exception(exc_type, exc_value, exc_traceback)
        t.insert(0,"[ERROR]:引数の形がちがいます")
        t.insert(1,"py RankingCheck.py [--drop] [-m 最大ランキング数] [-u URL] [-o DBファイル名] キーワード1 [キーワード2] [キーワード3] …")
        pprint.pprint(t, width=120,stream=sys.stderr)
        sys.exit(1)
    # 引数処理完了

    search(keyword, dbfile, url, max_ranking, drop_flg)

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as e:
        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        t = traceback.format_exception(exc_type, exc_value, exc_traceback)
        pprint.pprint(t, width=120,stream=sys.stderr)
        sys.exit(1)
    sys.exit(0)
