# ランキングチェッカー

与えられたキーワードのランキングチェックをおこないます。
このツールを作成した経緯については以下のサイトをごらんください。

[pythonでキーワードのランキングチェックツールを作成してみた](https://www.dmysd.net/news/archives/5)

## 事前準備

```sh
pip install sqlalchemy requests beautifulsoup4 lxml
```

## 実行方法

```sh
py RankingCheck.py [--drop] [-u URL] [-o DBファイル名] [-m 調査最大順位] キーワード1 [キーワード2] [キーワード3] …
```

- キーワードでGoogle検索をおこなった際の順位ランキングsqliteに出力する
- dropオプションを付与すると検索前にいったんデータベース上のテーブルをすべて削除する
- uオプションで自分の運営するサイトのURLを指定できる。DB上ではドキュメントの自ページフラグがTrueで登録される
- oオプションで出力先のSQLiteのファイル名を指定できる。指定しない場合にはデフォルト値"ranking.sqlite3"で出力される
- mオプションで何位まで調査するかを指定する
- []で囲まれているのは省略可能な引数
- 順位検索のHTMLは日時のフォルダが作成されその下に連番で保存される

## ER図

```mermaid
erDiagram
    T_SERACH_M ||--|{ T_SERACH : search_m_id
    T_SERACH ||--|{ T_RANKING : search_id
    T_DOC ||--|{ T_RANKING : doc_id
```

## シーケンス図

```plantuml
@startuml
actor コマンドライン as cmd
participant メイン処理 as main
participant 検索処理 as search
participant 検索開始 as search_start
participant 次頁検索 as search_next
participant DB登録更新処理 as db_upsert
database 検索マスタ as t_search_m
database 検索 as t_search
database ドキュメント as t_doc
database ランキング as t_ranking

cmd->>main: コマンドライン引数
    main->>search: 
        search->>t_search_m: 登録(キーワード)
            t_search_m->>search: 登録レコード
        search->>t_search: 登録(検索マスタid)
            t_search->>search: 登録レコード
        search->>search_start: 
            search_start->>db_upsert: 
                db_upsert->>t_doc: 登録/更新(ドキュメントurl)
                    t_doc->>db_upsert: 登録/更新レコード
                db_upsert->>t_ranking: 登録/更新(検索id,ドキュメントid)
                    t_ranking->>db_upsert: 登録/更新レコード
                db_upsert->>search_start: 
            alt 次へのリンクが存在する場合
            search_start->>search_next: 
                search_next->>db_upsert: 
                    db_upsert->>t_doc: 登録/更新(ドキュメントurl)
                        t_doc->>db_upsert: 登録/更新レコード
                    db_upsert->>t_ranking: 登録/更新(検索id,ドキュメントid)
                        t_ranking->>db_upsert: 登録/更新レコード
                    db_upsert->>search_next: 
                alt 次へのリンクが存在する場合
                    search_next->>search_next: 
                end
                search_next->>search_start: 
            end
            search_start->>search: 
        search->>main: 
    main->>cmd: 正常(0)、異常(1)
@enduml
```

## 変更履歴

|ver.|履歴|
|:--|:--|
|0.1|初版|
|0.2|キーワードを追跡しやすいようにモデルを変更|
|0.3|データ可視化ツールを入れる前準備としてモデルを別ファイルに移動|
