# -*- coding: utf-8 -*-

import sys
import os
import pprint
import traceback
import sqlalchemy
import json
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import datetime
from sqlalchemy.sql.expression import null
from RankingModels import Base, TSearchM, TSearch, TRanking, TDoc
from googleapiclient.discovery import build
from time import sleep

def __db_upsert(dbfile: str, keywords: list[str], response_list: list, my_url: str, drop_flg = False) -> int:
    """DB登録更新処理

    Google Search APIのrensponseを元に順位をDBに登録する処理をおこなう

    Parameters
    ----------
    session : scoped_session
        データベース接続のセッション
    t_search : TSearch
        検索テーブル
    response_list : list
        レスポンスリスト
    my_url : str
        自サイトのURL

    Returns
    -------
    ranking : int
        処理完了したランキング順位
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

        tab_keywords = "\t".join(keywords)
        t_search_m = TSearchM()
        t_search_m.keywords=tab_keywords
        t_search_m = TSearchM().upsert(t_search_m,session)

        t_search = TSearch()
        dttime = datetime.now()
        t_search.search_m_id=t_search_m.id
        t_search.search_datetime = dttime
        t_search = TSearch().upsert(t_search,session)



        ranking = 0
        for response in response_list:
            items=response.get("items")
            for item in items:
                ranking = ranking + 1
                link_text=item.get("formattedUrl")
                doc_title=item.get("title")

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
                    t_ranking = TRanking().insert(t_ranking, session)

    except Exception:
        raise
    else:
        session.close()
    finally:
        engine.dispose()

    return ranking


def search(apikey: str,engineid: str, keywords: list[str], dbfile: str, my_url: str, max_ranking: int, drop_flg: bool, output_base_dir:str = '.'):
    """検索処理

    検索処理をおこない結果をディクショナリの配列に格納し、jsonに保存後、DB登録更新処理を呼び出す。

    Parameters
    ----------
    apikey : str
        Google APIキー
    engineid : str
        Search Engine ID
    keywords : list[str]
        検索キーワードの配列
    dbfile : str
        データベースファイル名
    max_ranking : int
        何位までランキングを検索するか
    drop_flg : bool
        TrueのときテーブルをいったんDROPして作成しなおす
    """
    if len(keywords) == 0:
        return 0
    if max_ranking <= 0:
        return 0

    search_time = datetime.now()

    service = build("customsearch", "v1", developerKey=apikey)

    search_word=""
    for keyword in keywords:
        #search_word = "{} \"{}\"".format(search_word,keyword)
        search_word = search_word + " " + keyword
    
    wordjoin = "_".join(keywords)

    page_limit = 10
    start_index = 1
    # APIのレスポンスを格納する配列
    # ここに10ページ分の検索結果のjsonレスポンスが配列として格納される。
    response = []
    for n_page in range(0,page_limit):
        try:
            sleep(1)
            # cse().listの全てのパラメータを知りたい場合には以下を参照
            # https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list?hl=ja
            # 戻り値は以下を参照
            # https://developers.google.com/custom-search/v1/reference/rest/v1/Search?hl=ja
            res=service.cse().list(
                q=search_word,
                cx=engineid,
                lr='lang_ja',
                num=10,
                start=start_index
            ).execute()
            response.append(res)
            # start_indexを自ページのトップに設定
            next_page = res.get("queries").get("nextPage")
            if next_page is None:
                break
            elif len(next_page) <= 0:
                break
            start_index = next_page[0].get("startIndex")
            if start_index is not None:
                if max_ranking < start_index:
                    # 調べたい順位まで調べたらbreak
                    break
            else:
                # 10ページ未満で終わる場合例外は発生しここでbreakする
                break
        except Exception as e:
            (exc_type, exc_value, exc_traceback) = sys.exc_info()
            t = traceback.format_exception(exc_type, exc_value, exc_traceback)
            pprint.pprint(t, width=120,stream=sys.stderr)
            break

    # ランキングjsonデータ文字列生成
    ranking_datetime=search_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    ranking_json = {
        'ranking_datetime': ranking_datetime,
        'response': response
    }
    json_output_string = json.dumps(ranking_json, ensure_ascii=False)

    # フォルダ作成
    output_dir = os.path.join(search_time.strftime('%Y-%m-%d'),"Data")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # ファイル名取得
    output_file = os.path.join(output_base_dir, output_dir ,'response-' + wordjoin + '-' + search_time.strftime('%Y%m%d%H%M%S')+'.json')

    # ファイル書き込み
    with open(output_file, 'w', encoding='UTF-8') as f:
        f.write(json_output_string)

    # 検索結果のDB登録更新処理
    ranking = __db_upsert(dbfile, keywords, response, my_url, output_base_dir, drop_flg)

    return ranking


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
    max_ranking = 100
    drop_flg = False
    apikey = ""
    engineid = ""
    # 引数処理開始
    try:
        for i,arg in enumerate(argv):
            if skip == False and i > 0:
                if arg == '-db':
                    dbfile = argv[i+1]
                    skip = True
                elif arg == '--apikey':
                    apikey = argv[i+1]
                    skip = True
                elif arg == '--engineid':
                    key = argv[i+1]
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
            errlist.append("py RankingCheckAPI.py [--drop] [-m 最大ランキング数] [-u URL] [-db DBファイル名] キーワード1 [キーワード2] [キーワード3] …")
            pprint.pprint(errlist, width=120,stream=sys.stderr)
            sys.exit(1)
    except IndexError as e:
        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        t = traceback.format_exception(exc_type, exc_value, exc_traceback)
        t.insert(0,"[ERROR]:引数の形がちがいます")
        t.insert(1,"py RankingCheckAPI.py [--drop] [--apikey GCPのAPIキー] [--engineid GCP検索エンジンID] [-m 最大ランキング数] [-u URL] [-db DBファイル名] キーワード1 [キーワード2] [キーワード3] …")
        pprint.pprint(t, width=120,stream=sys.stderr)
        sys.exit(1)
    # 引数処理完了
    if len(apikey) == 0:
        apikey=os.environ.get('GCP_CUSTOM_SEARCH_API_KEY')
        if apikey is None:
            errlist=[]
            errlist.append("[ERROR]:Google API Keyが設定されていません。GCP_CUSTOM_SEARCH_API_KEY環境変数を設定するか--apikeyオプションにGCPのAPI Keyを設定してください")
            pprint.pprint(errlist, width=120,stream=sys.stderr)
            sys.exit(1)
    if len(engineid) == 0:
        engineid=os.environ.get('GCP_CUSTOM_SEARCH_ENGINE_ID')
        if engineid is None:
            errlist=[]
            errlist.append("[ERROR]:Engine IDが設定されていません。GCP_CUSTOM_SEARCH_ENGINE_ID環境変数を設定するか--engineidオプションにGCPの検索エンジンIDを設定してください")
            pprint.pprint(errlist, width=120,stream=sys.stderr)
            sys.exit(1)

    search(apikey, engineid, keyword, dbfile, url, max_ranking, drop_flg)

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as e:
        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        t = traceback.format_exception(exc_type, exc_value, exc_traceback)
        pprint.pprint(t, width=120,stream=sys.stderr)
        sys.exit(1)
    sys.exit(0)
