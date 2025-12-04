# Databricks notebook source
# MAGIC %md
# MAGIC # はじめに

# COMMAND ----------

# MAGIC %md
# MAGIC ## このノートブックは、[データサイエンス100本ノック](https://github.com/The-Japan-DataScientist-Society/100knocks-preprocess)を参考に、pysparkで構造化データを扱う際に必要なスキル習得を目的とした演習問題集です。# noqa

# COMMAND ----------

# MAGIC %md
# MAGIC ## 題材は、[原価精緻化データマート（通称：火星マート）](https://github.com/ML-ANALYSIS/percel-cost-refinement)になります。# noqa
# MAGIC そのため、本データセットではおそらく使用しないであろう処理や複雑な処理は今回対象外にしています。そのあたりについても知りたい方は[こちら](https://github.com/t-hashiguchi1995/100knock_pyspark/blob/main/preprocess_knock_Python_Spark.ipynb)を参考にしてください。# noqa
# MAGIC また、関数の説明は省いてますので、適宜google検索等を使用してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 不具合等がございましたら、小山（01158631@kuronekoyamato.co.jp)までお願いいたします。

# COMMAND ----------

# MAGIC %md
# MAGIC # 必要なライブラリのインポートをする。

# COMMAND ----------

import numpy as np
import scipy as sp
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.column import Column

# COMMAND ----------

# MAGIC %md
# MAGIC # データの読み込み

# COMMAND ----------

# MAGIC %md
# MAGIC ### 以下のセルを実行し、KBDからデータを抽出します。例として、2024年4月のデータを抽出します。
# MAGIC ただし、処理が重いため、同様のデータをparuet形式でblobに保存しているものの使用（セットアップ）を推奨します。

# COMMAND ----------

# 必要な年月を設定する。
target_ym = "202404"

# COMMAND ----------

# クエリを記載する。その際、f-string形式で行う。
query = f"""
SELECT *
FROM MART.DG_seichika_genka
WHERE FORMAT(集計日, 'yyyyMM') = '{target_ym}'
"""

# COMMAND ----------

# synapse接続に関する文字列を記載する。
url = (
    "jdbc:sqlserver://kbd2synapse001.database.windows.net:1433;"
    + "database=kbd2synapse001;user=DatabricksCommon@kbd2synapse001;"
    + "password=z65MtRsgay;encrypt=true;trustServerCertificate=false;;"
    + "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)

# COMMAND ----------

# データを抽出する。
genka_seichika_sdf_synapse = (
    spark.read.format("jdbc")
    .option("url", url)
    .option("numPartitions", 4)
    .option("fetchsize", 1000000000)
    .option("query", query)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## synapse接続回数を減らすためにblobに保存する。

# COMMAND ----------

# 各個人のパスを設定してください。
path = "/mnt/blob_sandbox/ydx/t_oyama/勉強会/tmp/"

genka_seichika_sdf_synapse.write.mode("overwrite").parquet(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ：処理が重い場合は、こちらにも同様のデータがあるため、こちらを参照ください。（読み込みが遅いことを想定しているため、デフォルトではTrue）

# COMMAND ----------

check = True
if check is True:
    path = f"/mnt/blob_sandbox/ydx/project/原価精緻化PJ/datamart/seichika_datamart/{target_ym}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## blobに保存したデータを読み込む

# COMMAND ----------

genka_seichika_sdf = spark.read.parquet(path)

# COMMAND ----------

# MAGIC %md
# MAGIC # 演習問題

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題1
# MAGIC 抽出したデータセットから、全項目の先頭10件を表示し、どのようなデータを保有しているか目視で確認せよ。

# COMMAND ----------

display(genka_seichika_sdf.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題2
# MAGIC 抽出したデータセットから、「伝票番号・統一コード・漢字商号・集計日・税抜合計運賃・定価・発店店所コード・発店事業所名・着地域名・サイズ品目名・サイズ・精緻化原価合計・ハキダシ合計」の順に選択し、10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.select(
        "伝票番号",
        "統一コード",
        "漢字商号",
        "集計日",
        "税抜合計運賃",
        "定価",
        "発店店所コード",
        "発店事業所名",
        "着地域名",
        "サイズ品目名",
        "サイズ",
        "精緻化原価合計",
        "ハキダシ合計",
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題3
# MAGIC 演習問題2で抽出したデータセットおよびカラムから、「統一コード」を「法人統一コード」に、「漢字商号」を「企業名」に変換しながら、10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.select(
        "伝票番号",
        F.col("統一コード").alias("法人統一コード"),
        F.col("漢字商号").alias("企業名"),
        "集計日",
        "税抜合計運賃",
        "定価",
        "発店店所コード",
        "発店事業所名",
        "着地域名",
        "サイズ品目名",
        "サイズ",
        "精緻化原価合計",
        "ハキダシ合計",
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題4
# MAGIC 以下の条件を満たすデータを抽出せよ。
# MAGIC
# MAGIC 顧客コードが"0013601566"

# COMMAND ----------

customer_code = "0013601566"

display(genka_seichika_sdf.filter(F.col("顧客コード") == customer_code))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題5
# MAGIC 以下の条件を満たすデータを抽出せよ。
# MAGIC
# MAGIC - 顧客コードが"0013601566"
# MAGIC - 精緻化原価合計が600以上

# COMMAND ----------

customer_code = "0013601566"
seichika_total_amount = 600

display(
    genka_seichika_sdf.filter(
        (F.col("顧客コード") == customer_code)
        & (F.col("精緻化原価合計") >= seichika_total_amount)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題6
# MAGIC 以下の条件を満たすデータを抽出せよ。
# MAGIC
# MAGIC - 顧客コードが"0013601566"
# MAGIC - 着主管支店コードが"033000"、または"133000"

# COMMAND ----------

customer_code = "0013601566"
shukan_code_1 = "033000"
shukan_code_2 = "133000"

display(
    genka_seichika_sdf.filter(
        (F.col("顧客コード") == customer_code)
        & (
            (F.col("着主管支店コード") == shukan_code_1)
            | (F.col("着主管支店コード") == shukan_code_2)
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題7
# MAGIC 以下の条件を満たすデータを抽出せよ。
# MAGIC
# MAGIC - 顧客コードが"0013601566"
# MAGIC - 着主管コードが"022000"以外

# COMMAND ----------

customer_code = "0013601566"
shukan_code_1 = "022000"

display(
    genka_seichika_sdf.filter(
        (F.col("顧客コード") == customer_code)
        & (F.col("着主管支店コード") != shukan_code_1)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題8
# MAGIC 以下の条件を満たすデータを抽出せよ。
# MAGIC
# MAGIC - 顧客コードが"0013601566"
# MAGIC - 精緻化原価合計が560以上、570以下

# COMMAND ----------

customer_code = "0013601566"
seichika_total_amount_min = 560
seichika_total_amount_max = 570

display(
    genka_seichika_sdf.filter(
        (F.col("顧客コード") == customer_code)
        & (
            (F.col("精緻化原価合計") >= seichika_total_amount_min)
            & (F.col("精緻化原価合計") <= seichika_total_amount_max)
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題9 ※難易度高め
# MAGIC 演習問題5において、出力結果を変えずにANDをORに書き換えよ。（少し難易度高め）

# COMMAND ----------

customer_code = "0013601566"
seichika_total_amount = 600

display(
    genka_seichika_sdf.filter(
        ~(
            (F.col("顧客コード") != customer_code)
            | (F.col("精緻化原価合計") < seichika_total_amount)
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題10
# MAGIC 発店店所コードが"098"から始まるデータを抽出し、上位10件を表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.filter(F.col("発店店所コード").like("098%")).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題11
# MAGIC 発主管支店コードが"600"で終わりのデータを抽出し、上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.filter(F.col("発主管支店コード").like("%600")).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題12
# MAGIC 「発ベース名」というカラムを抽出し、発ベース名の種類を表示させよ。（ヒント：重複削除を行う）

# COMMAND ----------

display(genka_seichika_sdf.select("発ベース名").drop_duplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題13
# MAGIC 集荷日が早い順に並べ替えを行い、上位10件表示させよ。

# COMMAND ----------

display(genka_seichika_sdf.orderBy(F.col("集荷日")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題13
# MAGIC 集荷日が遅い順に並べ替えを行い、上位10件表示させよ。

# COMMAND ----------

display(genka_seichika_sdf.orderBy(F.col("集荷日").desc()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題14
# MAGIC 顧客コードが"0013601566"のデータにおいて、精緻化原価合計が大きい順にランクを付与し、伝票番号・精緻化原価合計・ランクを表示させよ。なお、精緻化原価合計が等しい場合は同一順位を付与するものとする。

# COMMAND ----------

# Windowをセットする。
customer_code = "0013601566"
window_spec = Window.orderBy(F.col("精緻化原価合計").desc())

display(
    genka_seichika_sdf.filter(F.col("顧客コード") == customer_code)
    .withColumn("rank", F.rank().over(window_spec))
    .select("伝票番号", "精緻化原価合計", "rank")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題15
# MAGIC 顧客コードが"0013601566"のデータにおいて、精緻化原価合計が大きい順にランクを付与し、伝票番号・精緻化原価合計・ランクを表示させよ。なお、精緻化原価合計が等しい場合でも別順位を付与すること。

# COMMAND ----------

# Windowをセットする。
customer_code = "0013601566"
window_spec = Window.orderBy(F.col("精緻化原価合計").desc())

display(
    genka_seichika_sdf.filter(F.col("顧客コード") == customer_code)
    .withColumn("rank", F.row_number().over(window_spec))
    .select("伝票番号", "精緻化原価合計", "rank")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題16
# MAGIC 2024年4月分の件数をカウントせよ。（データセットの件数をカウントせよ。）

# COMMAND ----------

genka_seichika_sdf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題17
# MAGIC 精緻化原価合計とハキダシ合計の顧客コードごとの総計を計算し、上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード")
    .sum("精緻化原価合計", "ハキダシ合計")
    .withColumnRenamed(
        "sum(精緻化原価合計)", "精緻化原価合計_顧客コード別総計"
    )
    .withColumnRenamed("sum(ハキダシ合計)", "ハキダシ合計_顧客コード別総計")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題18
# MAGIC 顧客コードごとに精緻化原価合計が最も安いデータを抽出し、上位10件表示させよ。ただし、nullのデータを除くこと。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード").min("精緻化原価合計").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題19
# MAGIC 顧客コードごとに最も古い集計日と最も新しい集計日を抽出し、上位10件を表示させよ。

# COMMAND ----------

genka_seichika_sdf_min = genka_seichika_sdf.groupBy("顧客コード").agg(
    F.min("集計日")
)
genka_seichika_sdf_max = genka_seichika_sdf.groupBy("顧客コード").agg(
    F.max("集計日")
)

genka_seichika_sdf_merge = genka_seichika_sdf_min.join(
    genka_seichika_sdf_max, how="left", on="顧客コード"
)
display(genka_seichika_sdf_merge.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題20
# MAGIC 顧客コードごとの精緻化原価合計の平均値を計算し、降順で上位10件を表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード")
    .avg("精緻化原価合計")
    .orderBy(F.col("avg(精緻化原価合計)").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題21
# MAGIC 顧客コードごとの精緻化原価合計の中央値を計算し、降順で上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード")
    .agg(F.median("精緻化原価合計").alias("精緻化原価合計_中央値"))
    .orderBy(F.col("精緻化原価合計_中央値").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題22
# MAGIC 顧客コードごとの精緻化原価合計の最頻値を計算し、降順で上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード")
    .agg(F.mode("精緻化原価合計").alias("精緻化原価合計_最頻値"))
    .orderBy(F.col("精緻化原価合計_最頻値").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題23
# MAGIC 顧客コードごとの精緻化原価合計の標本分散を計算し、降順で上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード")
    .agg(F.variance("精緻化原価合計").alias("精緻化原価合計_標本分散"))
    .orderBy(F.col("精緻化原価合計_標本分散").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題24
# MAGIC 顧客コードごとの精緻化原価合計の標本標準偏差を計算し、降順で上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード")
    .agg(F.stddev("精緻化原価合計").alias("精緻化原価合計_標本標準偏差"))
    .orderBy(F.col("精緻化原価合計_標本標準偏差").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題25
# MAGIC 顧客コードの精緻化原価合計の平均値とハキダシ合計の平均値を計算し、精緻化原価合計→ハキダシ合計の順に降順で10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.groupBy("顧客コード")
    .agg(
        F.mean("精緻化原価合計").alias("精緻化原価合計_平均値"),
        F.mean("ハキダシ合計").alias("ハキダシ合計_平均値"),
    )
    .orderBy(
        F.col("精緻化原価合計_平均値").desc(),
        F.col("ハキダシ合計_平均値").desc(),
    )
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題26
# MAGIC 精緻化原価合計とハキダシ合計の差額（精緻化原価合計-ハキダシ合計）を計算した列を追加し、降順で上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.withColumn(
        "差額", F.col("精緻化原価合計") - F.col("ハキダシ合計")
    )
    .orderBy(F.col("差額").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題27
# MAGIC 精緻化原価合計がnullになっているデータに対して、「nullフラグ（nullの時：1, それ以外：0）」を付与し、顧客コード順に上位10件表示させよ。

# COMMAND ----------

display(
    genka_seichika_sdf.withColumn(
        "nullフラグ", F.when(F.col("精緻化原価合計").isNull(), 1).otherwise(0)
    ).orderBy(F.col("顧客コード"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題28 ※難易度高め
# MAGIC 精緻化原価合計がnullのデータは、計算対象のデータにnullが含まれていた場合はnullになる。これらに対してnullを0埋めし、再計算した結果を「精緻化原価合計_0埋め」という列で作成し、降順で上位10件表示させよ。ただし、表示させるデータは元の精緻化原価合計がnullのデータのみにする。
# MAGIC
# MAGIC 計算式は、発C集荷＋発C仕分＋発B横持＋発B仕分＋運行＋着B仕分＋着B横持＋着C仕分＋着C配達＋着C不在
# MAGIC

# COMMAND ----------

display(
    genka_seichika_sdf.withColumn(
        "精緻化原価合計_0埋め",
        F.coalesce(F.col("発C集荷"), F.lit(0))
        + F.coalesce(F.col("発C仕分"), F.lit(0))
        + F.coalesce(F.col("発B横持"), F.lit(0))
        + F.coalesce(F.col("発B仕分"), F.lit(0))
        + F.coalesce(F.col("運行"), F.lit(0))
        + F.coalesce(F.col("着B仕分"), F.lit(0))
        + F.coalesce(F.col("着B横持"), F.lit(0))
        + F.coalesce(F.col("着C仕分"), F.lit(0))
        + F.coalesce(F.col("着C配達"), F.lit(0))
        + F.coalesce(F.col("着C不在"), F.lit(0)),
    )
    .filter(F.col("精緻化原価合計").isNull())
    .orderBy(F.col("精緻化原価合計_0埋め").desc())
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題29 ※応用問題であり、難易度かなり高め
# MAGIC 統一コードが"300599072"（さとふる）のデータにおいて、発地域×着地域の件数を計算し、マトリクス表として出力せよ。ただし、件数が存在しない組み合わせに関しては0埋めを行う。（ヒント：これまでに出てきていないfirst関数及びpivot関数を使用する）
# MAGIC

# COMMAND ----------

customer_code = "300599072"

# 北海道～沖縄の順に並べ替える場合は、こちらを設定する。
region_name = [
    "北海道",
    "北東北",
    "南東北",
    "関東",
    # "東京", #データセットに東京のデータが含まれていないため
    "信越",
    "北陸",
    "中部",
    "関西",
    "中国",
    "四国",
    "九州",
    "沖縄",
]

satofuru_sdf = (
    genka_seichika_sdf.filter(F.col("統一コード") == customer_code)
    .groupBy("発地域名", "着地域名")
    .agg(F.count("伝票番号").alias("伝票番号件数"))
    .groupBy("発地域名")
    .pivot("着地域名")
    # sparkの便宜上、1つしか値がないとしてもfirst関数を用いて指定する。
    .agg(F.first("伝票番号件数"))
    .fillna(0)
    # 以下はフォーマットを成型するための処理である。
    .withColumn(
        "sort_key_A", F.array_position(F.lit(region_name), F.col("発地域名"))
    )
    .orderBy("sort_key_A")
    .drop("sort_key_A")
    .select(["発地域名"] + region_name)
    .withColumnRenamed("発地域名", "発地域名/着地域名")
)

display(satofuru_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 演習問題30
# MAGIC 演習問題29（もしくは28）で抽出したデータを自分のblobにcsv形式で保存せよ。（ヒント：デフォルトでは「part-00000-....csv」のような形式で出力されるため、pandas形式に変換し保存する。また、文字化けが発生するため、encodingを実施する。またpathは"/dbfs/mnt/datalake003/ydx/~"とする。

# COMMAND ----------

blob_path = "/dbfs/mnt/datalake003/ydx/t_oyama/勉強会/output/output_勉強会.csv"
satofuru_pd = satofuru_sdf.toPandas().to_csv(
    blob_path, index=False, header=True, encoding="shift_jis"
)
satofuru_pd.to_csv(blob_path, index=False, header=True, encoding="shift_jis")
