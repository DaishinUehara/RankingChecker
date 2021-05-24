# -*- coding: utf-8 -*-

import os
import sys
import pprint
import traceback
import sqlalchemy
from sqlalchemy.orm import scoped_session, sessionmaker
import plotly.graph_objects as go
from RankingModels import Base, TSearchM, TSearch, TRanking, TDoc
from datetime import datetime

def selectRanking(dbfile:str, keywords:list[str]):

    connect_string = "sqlite:///" + dbfile
    engine = sqlalchemy.create_engine(connect_string, echo=False) # SQLとデータを出力したい場合はecho=Trueにする

    try:
        session = scoped_session(
                    sessionmaker(
                        autocommit = False,
                        autoflush = True,
                        bind = engine))

        Base.query = session.query_property()

        Base.metadata.create_all(engine)

        # ランキングに出でくるサイトを全部洗い出し最新ランキングの高い順に並び替えた上でdictionaryにセット
        # この段階では
        result = session.query(
            TSearchM.keywords
            ,TSearch.search_m_id
            ,TDoc.id
            ,TDoc.title
            ,TSearch.search_datetime
            ,TRanking.ranking
        ).join(
            TSearch,TSearchM.id == TSearch.search_m_id
        ).join(
            TRanking,TSearch.id == TRanking.search_id
        ).join(
            TDoc,TRanking.doc_id == TDoc.id
        )
        if len(keywords) > 0:
            searchKeywords = "\t".join(keywords)
            result = result.filter(
                TSearchM.keywords == searchKeywords
            )

        result = result.order_by(
            TSearchM.keywords
            ,TSearch.search_datetime.desc()
            ,TRanking.ranking
            ,TDoc.title
        ).all()

        graph_dic={}
        for raw in result:
            graph_keyword='['+str(raw.search_m_id)+']'+ raw.keywords
            if graph_keyword not in graph_dic:
                # キーワードが存在しない場合。新しいグラフの描画
                # 日付を取得
                date_axis=[]
                result = session.query(
                    TSearch.search_datetime
                ).filter(
                    TSearch.search_m_id == raw.search_m_id
                ).order_by(
                    TSearch.search_datetime.desc()
                ).all()

                for dates in result:
                    date_axis.append(dates.search_datetime)

                site_dic={}
                keyword_dic={
                    "日付": date_axis
                    ,"サイト": site_dic
                }

                graph_dic[graph_keyword]=keyword_dic

            title = '[' + str(raw.id) + ']' + raw.title
            
            if title not in site_dic:
                # ここでランキング順位をすべてNoneでリセットしておく
                # python 3.7以降はdictが順序を保持するようになったためシンプルに突っ込む
                ranking_updown = [None] * len(date_axis)
                site_dic[title] = ranking_updown

            # 順位を取得
            ranking_updown = site_dic[title]
            # 日付と同じインデックス(順番の配列)に順位を入れる
            ranking_updown[date_axis.index(raw.search_datetime)] = raw.ranking

    except Exception:
        raise
    else:
        session.close()
    finally:
        engine.dispose()
    return graph_dic

def plot_datas(plotdatas:dict, output_base_dir:str = 'Plots'):
    for keyword, datas in plotdatas.items():
        fig = go.Figure()
        for key, data in datas.items():
            if key == "日付":
                xvalues = data
            elif key == "サイト":
                site_dic = data

        for site, rank in site_dic.items():
            fig.add_trace(go.Scatter(
                x=xvalues,
                y=rank,
                name=site
            ))
        fig.update_yaxes(autorange='reversed',dtick=5)
        fig.update_xaxes(tickformat="%Y-%m-%d",dtick='1 Day')
        fig.update_layout(title=keyword,xaxis_title="日付",yaxis_title="順位")
        filename=keyword.replace("\t","_") + ".html"

        dttime = datetime.now()
        output_dir = output_base_dir + os.sep + dttime.strftime('%Y-%m-%d') + os.sep + dttime.strftime('%H%M%S')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_dir + os.sep + filename,"w") as f:
            f.write(fig.to_html(full_html=True, include_plotlyjs='cdn'))

def main(argv: list[str]):

    skip = False
    dbfile="ranking.sqlite3"
    keywords = []
    try:
        for i,arg in enumerate(argv):
            if skip == False and i > 0:
                if arg == '-db':
                    dbfile = argv[i+1]
                    skip = True
                else:
                    keywords.append(arg)
            else:
                skip = False

        graph_dic=selectRanking(dbfile, keywords)
        plot_datas(graph_dic)

    except IndexError as e:
        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        t = traceback.format_exception(exc_type, exc_value, exc_traceback)
        t.insert(0,"[ERROR]:引数の形がちがいます")
        t.insert(1,"py RankingPlot.py [-db DBファイル名] [キーワード1] [キーワード2] [キーワード3] …")
        pprint.pprint(t, width=120,stream=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as e:
        (exc_type, exc_value, exc_traceback) = sys.exc_info()
        t = traceback.format_exception(exc_type, exc_value, exc_traceback)
        pprint.pprint(t, width=120,stream=sys.stderr)
        sys.exit(1)
    sys.exit(0)
