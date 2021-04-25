'''
Filename: /home/kellermann/git_repo/udacity-dend-capstone/lake.py
Path: /home/kellermann/git_repo/udacity-dend-capstone
Created Date: Wednesday, April 7th 2021, 8:10:38 pm
Author: Leandro Kellermann de Oliveira

Copyright (c) 2021 myProjects
'''
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf, concat, monotonically_increasing_id
from pyspark.sql.functions import translate, regexp_replace
from pyspark.sql.types import StringType
import unicodedata
import sys
import pandas as pd
import os
import configparser


def create_spark_session() -> SparkSession:
    """Method to create an SparkSession to connect to AWS EMR Cluster.
    Returns:
        SparkSession: SparkSession object that connects to AWS EMR Cluster.
    """
    session = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    session.conf.set('spark.executor.memory', '6g')
    return session


def make_trans():
    """Auxiliar method to remove accents from texts with Spark.

    Returns:
        str: the pair of strings where the first is the original string to be raplaced by the seccond.

    @Greetings:
        @ zero323
        https://pt.coredump.biz/questions/38359534/what-is-the-best-way-to-remove-accents-with-apache-spark-dataframes-in-pyspark
    """

    matching_string = ""
    replace_string = ""

    for i in range(ord(" "), sys.maxunicode):
        name = unicodedata.name(chr(i), "")
        if "WITH" in name:
            try:
                base = unicodedata.lookup(name.split(" WITH")[0])
                matching_string += chr(i)
                replace_string += base
            except KeyError:
                pass
    return matching_string, replace_string


def clean_text(c) -> str:
    """Method to replace non-ascii character for an ascii one.

    Args:
        c (str): the string with the character(s) we want to replace

    Returns:
        str: the string with ascii character(s).
    """
    matching_string, replace_string = make_trans()

    return translate(
        regexp_replace(c, "\p{M}", ""),

        matching_string, replace_string
    ).alias(c)


def remove_nonascii(c):
    """Method to remove non-ascii character for an ascii one.

    Args:
        c (str): the string with the character(s) we want to remove.

    Returns:
        str: the string without non-ascii characters.
    """

    matching_string, replace_string = make_trans()

    return translate(
        regexp_replace(c, r'[^A-Za-z]{1}', ""),
        matching_string, replace_string
    ).alias(c)


def preprocessing_geodatasets(input: str, output: str):
    """Method to process geographical data

    Args:
        input (str): path to input directory.
        output (str): path to output directory
    """

    # Preparing coordinates data:
    df = pd.read_csv(f'{input}coordinates.csv')
    obj_columns = ['GRANDES_RE', 'NOME_UF',
                   'NOME_MICRO', 'NOME_MESOR', 'NOME_MUNIC']

    for column in obj_columns:
        df[column] = df[column].str.replace(r'[^A-Za-z]{1}', '', regex=True)
        df[column] = df[column].str.lower()

    df['key'] = df.NOME_UF + df.NOME_MICRO
    df.dropna(inplace=True)

    key_columns = ['key',
                   'NOME_UF',
                   'CODIGO_UF',
                   'xcoord',
                   'ycoord',
                   'area_perimeter_area',
                   'area_perimeter_perimeter', ]

    df_key = df[key_columns]
    df_key.to_csv(f'{output}geo_info.csv', index=False)

    # Demographic data:
    df_dem = pd.read_excel(f'{input}POP2020_20210331.xls',
                           sheet_name='Municípios', header=1)
    df_dem.dropna(inplace=True)

    rename = {'UF': 'fed_unit',
              'COD. UF': 'fu_code',
              'COD. MUNIC': 'city_code',
              'NOME DO MUNICÍPIO': 'city_name',
              'POPULAÇÃO ESTIMADA': 'pop_est', }
    df_dem = df_dem.rename(rename, axis=1)
    df_dem.to_csv(f'{output}demographic_info.csv', index=False)


def create_place_dim(session: SparkSession, input: str, output: str) -> None:
    geo = f'{input}/geo_data/geo_info.csv'
    dem = f'{input}/geo_data/demographic_info.csv'
    uf = f'{input}/geo_data/uf_brazil.csv'
    dis = f'{input}/geo_data/RELATORIO_DTB_BRASIL_DISTRITO.xls'
    lower = udf(lambda s: s.lower() if s is not None else None, StringType())

    df_geo = session.read.load(geo,
                               format='com.databricks.spark.csv',
                               inferSchema='true',
                               header='true',
                               ignoreLeadingWhiteSpace='true',
                               ignoreTrailingWhiteSpace='true',
                               ).cache()

    df_dem = session.read.load(dem,
                               format='com.databricks.spark.csv',
                               inferSchema='true',
                               header='true',
                               ignoreLeadingWhiteSpace='true',
                               ignoreTrailingWhiteSpace='true',
                               ).cache()

    df_dis = pd.read_excel(dis)
    sql_ctx = SQLContext(session)

    df_dis = sql_ctx.createDataFrame(df_dis)
    df_dis.show()

    select_dis = ['cast(`Microrregião Geográfica` as int) as cod_microreg',
                  'cast(`Município` as int) as cod_city',
                  'cast(`Código Município Completo` as int) as cod_city_full',
                  'cast(Distrito as int) as cod_district',
                  'cast(`Código de Distrito Completo` as int) as cod_district_full',
                  ]
    df_dis = df_dis.selectExpr(select_dis)
    df_dis.show()

    select_exp_geodata = ['key',
                          'NOME_UF as fed_unit',
                          'cast(CODIGO_UF as int) as fu_code',
                          'cast(xcoord as float) as latitude',
                          'cast(ycoord as float) longitude',
                          'cast(area_perimeter_area as float) as location_area',
                          'cast(area_perimeter_perimeter as float) as location_perimeter'
                          ]
    select_exp_dem = ['key',
                      'fed_unit',
                      'cast(city_code as int)  as city_code',
                      'city_name',
                      'cast(pop_est as int) as pop_est', ]

    df_geodata = df_geo.selectExpr(select_exp_geodata)\
        .dropDuplicates().dropna()

    df_geodata.show(5)
    print(df_geodata.dtypes)

    df_dem2 = df_dem.withColumn('key', concat('fed_unit', 'city_name'))\
        .withColumn('key', remove_nonascii('key'))\
        .withColumn('key', lower('key'))

    df_demograph = df_dem2.selectExpr(select_exp_dem).dropna()

    df_demograph.show(5)

    df_uf = session.read.load(uf,
                              format='com.databricks.spark.csv',
                              inferSchema='true',
                              header='true',
                              ignoreLeadingWhiteSpace='true',
                              ignoreTrailingWhiteSpace='true',
                              ).cache()
    df_uf.show(5)

    df_join = df_geodata.join(df_demograph,
                              (df_geodata.key == df_demograph.key))\
        .join(df_uf, (df_geodata.fu_code == df_uf.code_uf))\
        .withColumn('uf_name_nonascii', remove_nonascii('uf_name'))\
        .withColumn('city_name_nonascii', remove_nonascii('city_name'))\
        .withColumn('key_2', concat('uf_name_nonascii', 'city_name_nonascii'))\
        .withColumn('key_2', lower('key_2'))\
        .withColumn('uf_name_clean', clean_text('uf_name'))\
        .withColumn('city_name_clean', clean_text('city_name'))\
        .drop(df_geodata.fed_unit)\
        .withColumn('key_3', concat('fed_unit', 'city_name_clean'))\
        .withColumn('key_3', lower('key_3'))

    df_join = df_join.select(df_demograph.key,
                             df_join.key_2,
                             df_join.key_3,
                             df_demograph.fed_unit,
                             df_geodata.fu_code,
                             df_uf.uf_name,
                             df_demograph.city_name,
                             df_demograph.city_code,
                             df_geodata.latitude,
                             df_geodata.longitude,
                             df_geodata.location_area,
                             df_geodata.location_perimeter,
                             df_demograph.pop_est,
                             )\
        .join(df_dis, (df_demograph.city_code == df_dis.cod_city))\
        .dropDuplicates()

    df_join.show(5)

    df_join.write.mode('overwrite')\
        .partitionBy('key').parquet(output+'/geographic_info')
    print('\n############\nCreated geographic_info dimension table.\n############\n\n')


def create_fact_tables(session: SparkSession, input: str, output: str):
    """Method to create fact tables illness and characteristics. It also creates
    health_units dimension table.

    Args:
        session (SparkSession): the SparkSession object.
        input (str): parent folder with input data.
        output (str): parent folder to output data.
    """
    city_path = rf'{output}/geographic_info/*/*.parquet'
    path = f'{input}/notification/*.csv'
    lower = udf(lambda s: s.lower() if s is not None else None, StringType())

    df_city = session.read.parquet(city_path)

    df = session.read.load(path,
                           format='com.databricks.spark.csv',
                           inferSchema='true',
                           sep=',',
                           header='true',
                           mode='DROPMALFORMED',
                           ignoreLeadingWhiteSpace='true',
                           ignoreTrailingWhiteSpace='true',
                           )

    df = df.withColumn('notification_id', monotonically_increasing_id())

    df = df.withColumn('key_city_not', concat('SG_UF_NOT', 'ID_MUNICIP'))\
        .withColumn('key_city_not', lower('key_city_not'))
    df.show(5)

    # print(f'Number of rows and columns: ({df_rowcount},{df_colcount})')
    select_health = ['cast(CO_UNI_NOT as int) as id_health_unit',
                     'trim(ID_UNIDADE) as nm_health_unit',
                     'trim(ID_MUNICIP) as nm_city',
                     'cast(CO_MUN_NOT as int) as id_city',
                     'cast(CO_REGIONA as int) as id_region',
                     'ID_REGIONA as nm_region',
                     ]

    # Health unit dimension table:
    df_hu = df.selectExpr(select_health)\
        .dropna(subset='id_health_unit')\
        .dropDuplicates()

    df_hu.write.mode('overwrite')\
        .partitionBy('nm_city').parquet(output+'health_units')
    df_hu.show(5)
    # Patient characteristic table:
    select_characteristics = ['notification_id',
                              'dt_notification',
                              'fl_covid19',
                              'res_covid19_test',
                              'id_health_unit',
                              # 'cast(CO_UNI_NOT as int) as id_health_unit_notific',
                              'CS_SEXO as cod_sex',
                              """to_date(DT_NASC, "dd/MM/YYYY") as dt_birth""",
                              'CS_GESTANT as qrt_pregnant',
                              'CS_RACA as cod_race',
                              'CS_ESCOL_N as cod_schooling',
                              'PAC_DSCBO as cod_profession',
                              'CO_PAIS as cod_country_hab',
                              'key_city_not',
                              'nm_fed_unit_not',
                              'fu_code as fu_code_not',
                              'nm_city as nm_city_not',
                              'latitude',
                              'longitude',
                              'location_area',
                              'location_perimeter',
                              'pop_est'
                              ]

    select_ill = ['notification_id',
                  'to_date(DT_NOTIFIC,"dd/MM/YYYY") as dt_notification',

                  """case when cast(SURTO_SG as int) = 1 then True
                    when cast(SURTO_SG as int) = 2 then False end as fl_covid19""",

                  """case when cast(FEBRE as int) = 1 then True
                  when cast(FEBRE as int) = 2 then False end as fl_fever""",

                  """case when cast(DISPNEIA as int) = 1 then True
                  when cast(DISPNEIA as int) = 2 then False end as fl_dyspnoea""",

                  """case when cast(DESC_RESP as int) = 1 then True
                  when cast(DESC_RESP as int) = 2 then False end as fl_breath_probl""",

                  """case when cast(SATURACAO as int) = 1 then True
                  when cast(SATURACAO as int) = 2 then False end as fl_o2_sat""",

                  """case when cast(DIARREIA as int) = 1 then True
                  when cast(DIARREIA as int) = 2 then False end as fl_diarr""",

                  """case when cast(VOMITO as int) = 1 then True
                  when cast(VOMITO as int) = 2 then False end as fl_vomit""",

                  """case when cast(DOR_ABD as int) = 1 then True
                  when cast(DOR_ABD as int) = 2 then False end as fl_abd_pain""",

                  """case when cast(FADIGA as int) = 1 then True
                  when cast(FADIGA as int) = 2 then False end as fl_fatig""",

                  """case when cast(PERD_OLFT as int) = 1 then True
                  when cast(PERD_OLFT as int) = 2 then False end as fl_smell_loss""",

                  """case when cast(PERD_PALA as int) = 1 then True
                  when cast(PERD_PALA as int) = 2 then False end as fl_taste_loss""",

                  """case when cast(FATOR_RISC as int) = 1 then True
                  when cast(FATOR_RISC as int) = 2 then False end as fl_risc""",

                  """case when cast(HOSPITAL as int) = 1 then True
                  when cast(HOSPITAL as int) = 2 then False end as fl_hospitalized""",

                  """case when cast(UTI as int) = 1 then True
                  when cast(UTI as int) = 2 then False end as fl_icu""",

                  """case when cast(HISTO_VGM as int) = 1 then True
                  when cast(HISTO_VGM as int) = 2 then false end as fl_int_trip""",

                  'cast(RES_IGG as int) as res_covid19_test',
                  ]

    # Create table with illness by notification_id
    df_ill = df.selectExpr(select_ill)

    df_ill.write.mode('overwrite')\
        .parquet(output+'illness')
    df_ill.show(5)

    # Columns from geographic_info to join with characteristics table:
    select_dim_city = ['key_3',
                       'fed_unit as nm_fed_unit_not',
                       'fu_code',
                       'city_name as nm_city',
                       'latitude',
                       'longitude',
                       'location_area',
                       'location_perimeter',
                       'pop_est']

    df_dim_city = df_city.selectExpr(select_dim_city)\
        .dropDuplicates()
    df_dim_city.show(5)

    # Joining characteristics table with geographical info
    df_characteristics = df.join(df_dim_city,
                                 (df.key_city_not == df_dim_city.key_3), 'inner')\
        .join(df_ill, (df.notification_id == df_ill.notification_id), 'inner')\
        .drop(df_ill.notification_id)\
        .join(df_hu, (df.CO_UNI_NOT == df_hu.id_health_unit), 'inner').drop(df_hu.nm_city)

    df_characteristics = df_characteristics.selectExpr(select_characteristics)

    # Write table:
    df_characteristics.write.mode('overwrite')\
        .partitionBy('nm_fed_unit_not').parquet(output+'/characteristics')

    df_characteristics.show(5)


def quality_check_parquet(session: SparkSession, directory: str) -> None:
    """Method to check data quality for dimension tables health_units, illness
    and fact table characteristics.

    Args:
        session (SparkSession): Spark session object.
        directory (str): directory where the table files are stored.

    Raises:
        ValueError: if the number of rows is smaller than one or\
            the number of columns is equal or smaller than two,
            then a ValueError is raised.
    """
    path_list = [f'{directory}health_units',    # Health units dimension table.
                 f'{directory}illness',         # Illness table.
                 # Geographic dimension table.
                 f'{directory}geographic_info/*/*.parquet',
                 # Characterisctics fact table.
                 f'{directory}characteristics',
                 ]
    dct = dict()
    for path in path_list:
        print(f'############\nRunning quality check for {path}:')
        df = session.read.parquet(path)
        df_rows = int(df.count())
        df_columns = int(len(df.columns))
        dct[f'{path}'] = (df_rows, df_columns)
        if df_rows < 1 or df_columns <= 2:
            # df_rws < 1 indicates no data was inserted.
            # Column <=2 indicates file is with wrong value separator.
            #  Here we need ';' not ',' as separator.
            raise ValueError(
                f'Invalid number of rows or columns: ({dct})')
        else:
            print(f'Number of rows and columns: ({df_rows},{df_columns})')
        print('############\n')

    # Illness table should be equal or greather than characteristics table.
    # The characteristics table can be smaller because of the key to join the
    # geographical information.
    if dct[f'{directory}characteristics'][0] > dct[f'{directory}illness'][0]:
        raise ValueError(
            """characteristics table cant be greater than illness table.""")


def main():

    # config = configparser.ConfigParser()
    # config.read('dl.cfg')

    # os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    # os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    # input = 's3a://input-bucket/'
    # output = 's3a://output-bucket/'

    input = 'input/'
    output = 'output/'

    input_geodata = 'input/geo_data/'

    preprocessing_geodatasets(input_geodata, input_geodata)

    session = create_spark_session()

    create_place_dim(session, input, output)
    create_fact_tables(session, input, output)

    quality_check_parquet(session, output)


if __name__ == '__main__':
    main()
