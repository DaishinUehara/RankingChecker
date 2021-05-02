# -*- coding: utf-8 -*-

import sys
import os
import pprint
import traceback
import sqlalchemy
import sqlite3
import requests
from sqlalchemy import Column, Integer, String, Date, Float, DateTime
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy.sql.expression import null

from sqlalchemy.sql.schema import UniqueConstraint


Base = declarative_base()

class TSearch(Base):
    """検索

    データベースの検索テーブルに対応するオブジェクト。
    検索を格納している。

    Attributes
    ----------
    id : int
        検索ID 自動採番
    search_datetime : datetime
        検索日時 外部キー(検索.id)
    """

    __tablename__ = 't_search'
    id = Column(Integer, primary_key=True, autoincrement=True)
    search_datetime = Column(DateTime, nullable=False)

    @staticmethod
    def upsert(t_search,session: scoped_session ):
        """登録更新処理

        search_datetimeで検索しレコードが存在しない場合にINSERTをおこなう。
        データが変更されていた場合はなにもおこなわない。
        (id以外に項目がsearch_datetimeのみのため処理の必要がない)。

        Parameters
        ----------
        t_search: TSearch
            登録更新対象のデータ
        session: scoped_session
            データベースへの接続セッション

        Returns
        -------
        ret_t_search: TSearch
            処理完了後のレコードが戻る
        """
        ret_t_search = session.query(TSearch).filter(TSearch.search_datetime==t_search.search_datetime).first()
        if ret_t_search is None:
            session.add(t_search)
            session.commit()
            ret_t_search = session.query(TSearch).filter(TSearch.search_datetime==t_search.search_datetime).first()
        return ret_t_search

class TKeyword(Base):
    """キーワード

    データベースのキーワードテーブルに対応するオブジェクト。
    検索ごとのキーワードを格納している。

    Attributes
    ----------
    id : int
        キーワードID 自動採番
    search_id : int
        検索ID 外部キー(検索.id)
    keyword : str
        検索キーワード
    """
    __tablename__ = 't_keyword'
    id = Column(Integer, primary_key=True, autoincrement=True)
    search_id = Column(Integer, nullable=False)
    keyword = Column(String(32), nullable=False)

    @staticmethod
    def upsert(t_keyword,session: scoped_session ):
        """登録更新処理

        search_idとkeywordで検索しレコードが存在しない場合にINSERTをおこなう。
        データが変更されていた場合はなにもおこなわない。
        (id以外に項目がsearch_idとkeywordのみのため処理の必要がない)。

        Parameters
        ----------
        t_keyword: TKeyword
            登録更新対象のデータ
        session: scoped_session
            データベースへの接続セッション

        Returns
        -------
        ret_t_keyword: TKeyword
            処理完了後のレコードが戻る
        """
        ret_t_keyword = session.query(TKeyword).filter(TKeyword.search_id==t_keyword.search_id).filter(TKeyword.keyword==t_keyword.keyword).first()
        if ret_t_keyword is None:
            session.add(t_keyword)
            session.commit()
            ret_t_keyword = session.query(TKeyword).filter(TKeyword.search_id==t_keyword.search_id).filter(TKeyword.keyword==t_keyword.keyword).first()
        return ret_t_keyword


class TRanking(Base):
    """ランキング

    データベースのランキングテーブルに対応するオブジェクト。
    検索ごとの順位と対応するドキュメントのidを格納している。
    検索IDとドキュメントIDのセットで自然キーとなっている

    Attributes
    ----------
    id : int
        ランキングID 自動採番
    search_id : int
        検索ID 外部キー(検索.id)
    doc_id : int
        ドキュメントID 外部キー(ドキュメント.id)
    ranking : int
        順位
    """
    __tablename__ = 't_ranking'
    __table_args__ = (UniqueConstraint('search_id','ranking'),{})    
    id = Column(Integer, primary_key=True, autoincrement=True)
    search_id = Column(Integer, nullable=False)
    doc_id = Column(Integer, nullable=False)
    ranking = Column(Integer, nullable=False)

    @staticmethod
    def insert(t_ranking,session: scoped_session ):
        """登録処理

        t_rankingのデータをINSERTする。

        Parameters
        ----------
        t_ranking: TRanking
            登録対象データ
        session: scoped_session
            データベースへの接続セッション

        Returns
        -------
        ret_t_ranking: TRanking
            処理完了後のレコードが戻る
        """
        session.add(t_ranking)
        session.commit()
        ret_t_ranking = session.query(TRanking).filter(TRanking.search_id==t_ranking.search_id).filter(TRanking.doc_id==t_ranking.doc_id).filter(TRanking.ranking==t_ranking.ranking).first()
        return ret_t_ranking

    @staticmethod
    def upsert(t_ranking,session: scoped_session ):
        """登録更新処理

        search_idとrankingで検索しレコードが存在しない場合にINSERTをおこなう。
        データが変更されていた場合はUPDATE処理をおこなう。

        Parameters
        ----------
        t_ranking: TRanking
            登録更新対象のデータ
        session: scoped_session
            データベースへの接続セッション

        Returns
        -------
        ret_t_ranking: TRanking
            処理完了後のレコードが戻る
        """
        ret_t_ranking = session.query(TRanking).filter(TRanking.search_id==t_ranking.search_id).filter(TRanking.ranking==t_ranking.ranking).first()
        if ret_t_ranking is None:
            session.add(t_ranking)
            session.commit()
            ret_t_ranking = session.query(TRanking).filter(TRanking.search_id==t_ranking.search_id).filter(TRanking.ranking==t_ranking.ranking).first()
        elif ret_t_ranking.doc_id != t_ranking.doc_id:
            ret_t_ranking.doc_id = t_ranking.doc_id
            session.commit()
        return ret_t_ranking

    @staticmethod
    def hasRanking(t_ranking,session: scoped_session ):
        """ドキュメントランキング登録確認処理

        search_idとdoc_idで検索しレコードが存在しているか確認する。

        Parameters
        ----------
        t_ranking: TRanking
            対象のデータ
        session: scoped_session
            データベースへの接続セッション

        Returns
        -------
        True: bool
            登録あり
        False: bool
            登録なし
        """
        ret_t_ranking = session.query(TRanking).filter(TRanking.search_id==t_ranking.search_id).filter(TRanking.doc_id==t_ranking.doc_id).first()
        if ret_t_ranking is None:
            return False
        else:
            return True

class TDoc(Base):
    """ドキュメント

    データベースのドキュメントテーブルに対応するオブジェクト

    Attributes
    ----------
    id : int
        ドキュメントID 自動採番
    link_url : str
        ドキュメントのURL、自然キー
    title : str
        検索結果から取得したページのタイトル
    mypage_flg : bool
        自分のページかどうかのフラグ。自ページのときTrue
    """
    __tablename__ = 't_doc'
    id = Column(Integer, primary_key=True, autoincrement=True)
    link_url = Column(String(2083), nullable=False)
    title = Column(String(128))
    mypage_flg = Column(Integer, nullable=False)

    @staticmethod
    def upsert(t_doc,session: scoped_session ):
        """登録更新処理

        URLで検索しレコードが存在しない場合にINSERTをおこなう。
        データが変更されていた場合はUPDATE処理をおこなう。

        Parameters
        ----------
        t_doc: TDoc
            登録更新対象のデータ
        session: scoped_session
            データベースへの接続セッション

        Returns
        -------
        ret_t_doc: TDoc
            処理完了後のレコードが戻る
        """
        ret_t_doc = session.query(TDoc).filter(TDoc.link_url==t_doc.link_url).first()
        if ret_t_doc is None:
            session.add(t_doc)
            session.commit()
            ret_t_doc = session.query(TDoc).filter(TDoc.link_url==t_doc.link_url).first()
        elif ret_t_doc.title != t_doc.title or ret_t_doc.mypage_flg != t_doc.mypage_flg:
            ret_t_doc.title = t_doc.title
            ret_t_doc.mypage_flg = t_doc.mypage_flg
            session.commit()
        return ret_t_doc


def __db_upsert(session: scoped_session,soup: BeautifulSoup, t_search: TSearch, ranking: int, max_ranking: int,search_time: datetime, my_url: str) -> int:
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

    search_time_string = search_time.strftime('%Y%m%d_%H%M%S')
    if not os.path.exists(search_time_string):
        os.makedirs(search_time_string)
    with open("{}{}bs_{}.html".format(search_time_string,os.sep,str(ranking)), 'w', encoding='UTF-8') as f:
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

def __search_next(session: scoped_session,t_search: TSearch, link_text: str, ranking: int, max_ranking: int,search_time: datetime, my_url: str) -> int:
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

    ret_ranking = __db_upsert(session, soup, t_search, ranking, max_ranking, search_time, my_url)
    if ret_ranking >= max_ranking:
        return ret_ranking

    a = soup.select_one("a[aria-label='次のページ']")
    if a is None:
        return ret_ranking

    link_text=a.get("href")
    return __search_next(session, t_search, link_text, ret_ranking, max_ranking, search_time, my_url)


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
        search_word = "{} \"{}\"".format(search_word,keyword)

    r = requests.get(google_url,params={'q': search_word})
    soup = BeautifulSoup(r.text, 'lxml') #要素を抽出
    ranking=0
    ret_ranking = __db_upsert(session, soup, t_search, ranking, max_ranking, search_time, my_url)
    if ret_ranking >= max_ranking:
        return ret_ranking

    a = soup.select_one("a[aria-label='次のページ']")
    if a is None:
        return ret_ranking
    link_text=a.get("href")
    return __search_next(session, t_search, link_text, ret_ranking, max_ranking, search_time, my_url)

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
        t_search = TSearch()
        dttime = datetime.now()
        t_search.search_datetime = dttime
        t_search = TSearch().upsert(t_search,session)
        for keyword in keywords:
            t_keyword = TKeyword()
            t_keyword.keyword = keyword
            t_keyword.search_id = t_search.id
            t_keyword = TKeyword().upsert(t_keyword,session)
        
        __search_start(session, t_search, keywords,max_ranking, dttime ,url)

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
