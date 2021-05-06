# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, Date, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql.schema import UniqueConstraint

Base = declarative_base()


class TSearchM(Base):
    """検索マスタ

    データベースの検索テーブルに対応するオブジェクト。
    検索を格納している。

    Attributes
    ----------
    id : int
        検索ID 自動採番
    search_datetime : datetime
        検索マスタID 外部キー(検索.id)
    keywords : text
        検索日時
    """

    __tablename__ = 't_search_m'
    id = Column(Integer, primary_key=True, autoincrement=True)
    keywords = Column(String(256), nullable=False)

    @staticmethod
    def upsert(t_search_m,session: scoped_session ):
        """登録更新処理

        keywordsで検索しレコードが存在しない場合にINSERTをおこなう。
        データが変更されていた場合はなにもおこなわない。
        (id以外に項目がkeywordsのみのため処理の必要がない)。

        Parameters
        ----------
        t_search_m: TSearchM
            登録更新対象のデータ
        session: scoped_session
            データベースへの接続セッション

        Returns
        -------
        ret_t_search_m: TSearchM
            処理完了後のレコードが戻る
        """
        ret_t_search_m = session.query(TSearchM).filter(TSearchM.keywords==t_search_m.keywords).first()
        if ret_t_search_m is not None:
            return ret_t_search_m
        else:
            session.add(t_search_m)
            session.commit()
            return t_search_m # commit後なのでidが採番されている


class TSearch(Base):
    """検索

    データベースの検索テーブルに対応するオブジェクト。
    検索を格納している。

    Attributes
    ----------
    id : int
        検索ID 自動採番
    search_datetime : datetime
        検索マスタID 外部キー(検索.id)
    search_datetime : datetime
        検索日時
    """

    __tablename__ = 't_search'
    id = Column(Integer, primary_key=True, autoincrement=True)
    search_m_id = Column(Integer, nullable=False)
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
        if ret_t_search is not None:
            return ret_t_search
        else:
            session.add(t_search)
            session.commit()
            return t_search # commit後なのでidが採番されている

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
        return t_ranking

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
            return t_ranking
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
            return t_doc
        elif ret_t_doc.title != t_doc.title or ret_t_doc.mypage_flg != t_doc.mypage_flg:
            ret_t_doc.title = t_doc.title
            ret_t_doc.mypage_flg = t_doc.mypage_flg
            session.commit()
        return ret_t_doc

