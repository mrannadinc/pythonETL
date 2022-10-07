import logging
import os
import shutil
import sqlalchemy

import sys
import time
from selenium import webdriver

import pandas as pd
from sqlalchemy import *
from sshtunnel import SSHTunnelForwarder


def vertica_connection():
    vertica_server = SSHTunnelForwarder(
        '************',
        ssh_username="********",
        ssh_password="***********",
        remote_bind_address=("************", 5434)
    )

    vertica_server.start()

    database_engine = create_engine("vertica+vertica_python://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('*******',
                                                                                                       '*********',
                                                                                                       '**********',
                                                                                                       vertica_server.local_bind_port,
                                                                                                       '***********'))

    return database_engine


def get_chrome_driver():
    option = webdriver.ChromeOptions()
    # option.add_argument("window-size=1980,1080")
    # option.headless = True
    driver = webdriver.Chrome(r"chromedriver.exe", options=option)
    driver.maximize_window()
    driver.get(
        "https://evds2.tcmb.gov.tr/index.php?/evds/serieMarket")  # url that data will fetch from there
    return driver


def select_main_title(driver, position):
    driver.find_element_by_xpath(f"//*[@id='dataGroupAccordion']/div[{position}]/div[1]").click()
    time.sleep(3)


def select_list_title(driver, group, title_index):
    driver.find_element_by_xpath(f"//*[@id='data-group-{group}-{title_index}']/a[2]").click()
    time.sleep(3)


def select_series(driver, series_index, for_all):
    if for_all:
        driver.find_element_by_xpath("//*[@id='serieSelectionTable']/thead/tr/th[1]/label/span").click()
        time.sleep(3)
    else:
        driver.find_element_by_xpath(f"//*[@id='serieMarketTableBody']/tr[1]/td[{series_index}]/label/span").click()
        time.sleep(3)


def click_formula_button(driver):
    driver.find_element_by_xpath(
        "//*[@id='serieMarketTable']/div/div[2]/div[1]/div[2]/span/div/button/span").click()
    time.sleep(3)


def select_formula(driver, formula_index):
    driver.find_element_by_xpath(
        f"//*[@id='serieMarketTable']/div/div[2]/div[1]/div[2]/span/div/ul/li[{formula_index}]/a/label").click()
    time.sleep(3)


def click_add_button(driver):
    driver.find_element_by_xpath(
        "// *[ @ id = 'serieMarketTable'] / div / div[2] / div[1] / div[3] / div / div / a").click()
    time.sleep(10)


def create_download_report(driver):
    # click create report button
    driver.find_element_by_xpath(
        "//*[@id='serieMarketGeneralOptions']/div/div[1]/div[2]/div[1]/button").click()
    time.sleep(3)
    driver.find_element_by_xpath("//*[@id='excelButton_']/div/i").click()
    time.sleep(3)
    driver.find_element_by_xpath("// *[ @ id = 'evdsDscModalButtonDownload'] / i").click()
    time.sleep(3)


def clear_chosen_list(driver):
    driver.find_element_by_xpath(
        "//*[@id='shoppingCartList']/a").click()
    time.sleep(3)


def rename_file(new_name):
    os.rename("EVDS.xlsx",
              f"{new_name}.xlsx")
    shutil.move(f"{new_name}.xlsx",
                f"{new_name}.xlsx")


class DataFetch:
    def __init__(self):
        print("Starting Fetch: ")
        print("Selenium Chrome Driver Adding...")
        self.get_chrome_driver = get_chrome_driver()
        first_time = time.time()
        self.gsyih_hacim_degisim_oran = self.gsyih_hacim_degisim_oran()
        second_time = time.time()
        print("--- %s fisrt take fetch first installing --- " % (second_time - first_time))
        third_time = time.time()
        self.total_export = self.total_export()
        fourth_time = time.time()
        print("--- %s second take fetch first installing --- " % (fourth_time - third_time))
        fifth_time = time.time()
        self.total_import = self.total_import()
        sixth_time = time.time()
        print("--- %s third take fetch first installing --- " % (sixth_time - fifth_time))
        seventh_time = time.time()
        self.industrial_production_index = self.industrial_production_index()
        eight_time = time.time()
        print("--- %s fourth take fetch first installing --- " % (eight_time - seventh_time))
        ninth = time.time()
        self.tufe_ufe_numbers = self.tufe_ufe_numbers()
        tenth = time.time()
        print("--- %s fifth take fetch first installing --- " % (tenth - ninth))
        eleventh = time.time()
        self.nufusun_isgucu_durumu = self.nufusun_isgucu_durumu()
        twelfth = time.time()
        print("--- %s sixth take fetch first installing --- " % (twelfth - eleventh))
        thirteenth = time.time()
        self.foreign_currency = self.foreign_currency()
        fourteenth = time.time()
        print("--- %s seventh take fetch first installing --- " % (fourteenth - thirteenth))
        fifteenth = time.time()
        self.confidence_index = self.confidence_index()
        sixteenth = time.time()
        print("--- %s seventh take fetch first installing --- " % (sixteenth - fifteenth))
        self.get_chrome_driver.close()

    def gsyih_hacim_degisim_oran(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        select_main_title(driver, 17)  # Üretim

        select_list_title(driver, 21, 6002)
        select_series(driver, 0, True)
        click_add_button(driver)
        create_download_report(driver)

        new_name = "gsyih_hacim_degisim_oran"
        rename_file(new_name)
        clear_chosen_list(driver)
        time.sleep(3)

    def industrial_production_index(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        select_main_title(driver, 17)  # Üretim

        select_list_title(driver, 21, 5038)
        select_series(driver, 1, False)
        click_add_button(driver)
        create_download_report(driver)

        new_name = "industrial_production_index"
        rename_file(new_name)
        clear_chosen_list(driver)
        time.sleep(3)

    def total_export(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        select_main_title(driver, 6)  # Dış ticaret istatistikleri

        list_title = [5118, 5120, 5985, 5986]

        for index in list_title:
            select_list_title(driver, 19, index)
            select_series(driver, 1, False)
            click_add_button(driver)

        create_download_report(driver)
        new_name = "total_export"
        rename_file(new_name)
        time.sleep(3)
        clear_chosen_list(driver)
        time.sleep(3)

    def total_import(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        select_main_title(driver, 6)  # Dış ticaret istatistikleri

        list_title = [5119, 5121, 5990, 5991]

        for index in list_title:
            select_list_title(driver, 19, index)
            select_series(driver, 1, False)
            click_add_button(driver)

        create_download_report(driver)
        new_name = "total_import"
        rename_file(new_name)
        clear_chosen_list(driver)
        time.sleep(3)

    def tufe_ufe_numbers(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        select_main_title(driver, 15)  # Fiyat endeksleri
        list_title = [5938, 5941]
        formula_list = [1, 4]
        for title_index in list_title:
            select_list_title(driver, 14, title_index)
            select_series(driver, 1, False)
            if title_index == 5938:
                click_formula_button(driver)
                for index in formula_list:
                    select_formula(driver, index)
            click_add_button(driver)

        create_download_report(driver)
        new_name = "tufe_ufe_numbers"
        rename_file(new_name)
        clear_chosen_list(driver)
        time.sleep(3)

    def nufusun_isgucu_durumu(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        select_main_title(driver, 20)  # istihdam

        select_list_title(driver, 23, 6017)
        select_series(driver, 0, True)
        click_formula_button(driver)
        formula_list = [4, 1]
        for formula_index in formula_list:
            select_formula(driver, formula_index)
        click_add_button(driver)

        create_download_report(driver)
        new_name = "nufusun_isgucu_durumu"
        rename_file(new_name)
        clear_chosen_list(driver)
        time.sleep(3)

    def foreign_currency(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        main_list = [1, 16]
        title_list = [5863, 5849]
        data_group = [2, 25]
        for main_index in main_list:
            select_main_title(driver, main_index)
            for title_index in title_list:
                if data_group == 2:
                    select_list_title(driver, 2, title_index)
                    select_series(driver, 0, True)
                    click_add_button(driver)
                else:
                    select_list_title(driver, 25, title_index)
                    select_series(driver, 3, False)
                    click_add_button(driver)
        create_download_report(driver)

        new_name = "foreign_currency"
        rename_file(new_name)
        clear_chosen_list(driver)
        time.sleep(3)

    def confidence_index(self):
        driver = self.get_chrome_driver
        time.sleep(5)
        select_main_title(driver, 12)  # anketler
        title_list = [5958, 5968]
        for index in title_list:
            select_list_title(driver, 15, index)
            select_series(driver, 1, False)
            click_add_button(driver)

        new_name = "confidence_index"
        rename_file(new_name)
        clear_chosen_list(driver)
        time.sleep(3)


class InsertData:
    def __init__(self):
        self.vertica_connection = vertica_connection()
        self.drop_all_table()

    def drop_all_table(self):
        print("All before fetched tables are dropping...")
        drop_list = ['gsyih_hacim_degisim_oran', 'total_export', 'total_import',
                     'industrial_production_index', 'tufe_ufe_numbers', 'nufusun_isgucu_durumu',
                     'tcmb_politika_faizi', 'satin_alma_mudurleri_endeksi', 'foreign_currency']
        for row in drop_list:
            try:
                self.vertica_connection.execute(f"drop table {row} cascade")
            except Exception as e:
                print(e)
        print("All tables are dropped!")

    def gsyih_hasila_degisim_orani_insert(self):
        file_path = "gsyih_hacim_degisim_oran.xlsx"

        df = pd.read_excel(file_path)

        df_tarih = pd.DataFrame(df['Tarih'])

        rows_with_nan = []
        for index, row in df_tarih.iterrows():
            is_nan_series = row.isnull()
            if is_nan_series.any():
                rows_with_nan.append(index)

        df_mn = df.drop(df.index[rows_with_nan[0]:100000000])
        df_mn.columns = ['yil',
                         'Yerleşik hanehalklarının tüketimi',
                         'Hanehalkına hizmet eden kar amacı olmayan kuruluşların tüketimi',
                         'Devletin nihai tüketim harcamaları',
                         'Gayrisafi sabit sermaye oluşumu',
                         'Mal ve hizmet ihracatı',
                         '(Eksi) Mal ve hizmet ithalatı',
                         'Gayrisafi yurtiçi hasıla']
        df_final = pd.melt(df_mn, id_vars=['yil'], value_vars=['Yerleşik hanehalklarının tüketimi',
                                                               'Hanehalkına hizmet eden kar amacı olmayan kuruluşların tüketimi',
                                                               'Devletin nihai tüketim harcamaları',
                                                               'Gayrisafi sabit sermaye oluşumu',
                                                               'Mal ve hizmet ihracatı',
                                                               '(Eksi) Mal ve hizmet ithalatı',
                                                               'Gayrisafi yurtiçi hasıla'],
                           var_name='harcama_bilesenleri', value_name='hacim_bin_tl')
        print(df_final)

        df_final.to_sql('gsyih_hacim_degisim_oran', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'yil': sqlalchemy.types.VARCHAR(length=20),
                                                 'harcama_bilesenleri': sqlalchemy.types.VARCHAR(length=100),
                                                 'hacim_bin_tl': sqlalchemy.types.Numeric()})
        print("Database insert completed - gsyih_hacim_degisim_oran")

    def update_gsyih_hacim_degisim_oran(self):
        print("gsyih_hacim_degisim_oran updating...")
        self.vertica_connection.execute(
            "ALTER TABLE gsyih_hacim_degisim_oran ADD COLUMN ceyrek varchar(10)")
        self.vertica_connection.execute(
            f"update gsyih_hacim_degisim_oran "
            f"set ceyrek =  trim('-' from right(yil, instr(right(yil, instr(yil, '-')),'-'))) "
            "where yil is not null")
        print("ceyrek column is updated!")
        self.vertica_connection.execute(
            "update gsyih_hacim_degisim_oran "
            "set yil = trim('-' from (left(yil, instr(yil,'-')))) "
            "where yil is not null;")
        print("yil column is updated!")

    def total_export_insert(self):
        file_path = "total_export.xlsx"

        df = pd.read_excel(file_path)

        df_tarih = pd.DataFrame(df['Tarih'])

        rows_with_nan = []
        for index, row in df_tarih.iterrows():
            is_nan_series = row.isnull()
            if is_nan_series.any():
                rows_with_nan.append(index)

        df_mn = df.drop(df.index[rows_with_nan[0]:100000000])

        df_mn.columns = ['tarih',
                         'ihracat (BEC)',
                         'ihracat (ISICREV4)',
                         'birim deger endeksi (BEC)',
                         'birim deger endeksi (ISICREV4)']
        df_final = pd.melt(df_mn, id_vars=['tarih'], value_vars=['ihracat (BEC)',
                                                                 'ihracat (ISICREV4)',
                                                                 'birim deger endeksi (BEC)',
                                                                 'birim deger endeksi (ISICREV4)'],
                           var_name='siniflandirma', value_name='deger')

        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)
        print(df_final)

        df_final.to_sql('total_export', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'siniflandirma': sqlalchemy.types.VARCHAR(length=50),
                                                 'deger': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - total_export")

    def total_import_insert(self):
        file_path = "total_import.xlsx"

        df = pd.read_excel(file_path)

        df_tarih = pd.DataFrame(df['Tarih'])

        rows_with_nan = []
        for index, row in df_tarih.iterrows():
            is_nan_series = row.isnull()
            if is_nan_series.any():
                rows_with_nan.append(index)

        df_mn = df.drop(df.index[rows_with_nan[0]:100000000])

        df_mn.columns = ['tarih',
                         'ithalat (BEC)',
                         'ithalat (ISICREV4)',
                         'birim deger endeksi (BEC)',
                         'birim deger endeksi (ISICREV4)']
        df_final = df_mn.melt('tarih').sort_values('tarih')
        df_final.columns = ['tarih', 'siniflandirma', 'deger']

        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)
        print(df_final)

        df_final.to_sql('total_import', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'siniflandirma': sqlalchemy.types.VARCHAR(length=50),
                                                 'deger': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - total_import")

    def industrial_production_index_insert(self):
        file_path = "industrial_production_index.xlsx"

        df = pd.read_excel(file_path)

        df_tarih = pd.DataFrame(df['Tarih'])

        rows_with_nan = []
        for index, row in df_tarih.iterrows():
            is_nan_series = row.isnull()
            if is_nan_series.any():
                rows_with_nan.append(index)

        df_new = df.drop(df.index[rows_with_nan[0]:100000000])
        df_final = df_new[['Tarih', 'TP SANAYREV4 Y1']]

        df_final.columns = ['tarih',
                            'toplam_sanayi_uretim_endeksi']
        print(df_final)
        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)
        print(df_final)

        df_final.to_sql('industrial_production_index', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'toplam_sanayi_uretim_endeksi': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - industrial_production_index")

    def tufe_ufe_numbers_insert(self):
        file_path = "tufe_ufe_numbers.xlsx"

        df = pd.read_excel(file_path)

        df_tarih = pd.DataFrame(df['Tarih'])

        rows_with_nan = []
        for index, row in df_tarih.iterrows():
            is_nan_series = row.isnull()
            if is_nan_series.any():
                rows_with_nan.append(index)

        df_final = df.drop(df.index[rows_with_nan[0]:100000000])

        df_final.columns = ['tarih',
                            'tufe',
                            'ufe']
        print(df_final)
        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)
        print(df_final)

        df_final.to_sql('tufe_ufe_numbers', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'tufe': sqlalchemy.types.Numeric(),
                                                 'ufe': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - tufe_ufe_numbers")

    def nufusun_isgucu_durumu_insert(self):
        file_path = "nufusun_isgucu_durumu.xlsx"

        df = pd.read_excel(file_path)

        df_tarih = pd.DataFrame(df['Tarih'])

        rows_with_nan = []
        for index, row in df_tarih.iterrows():
            is_nan_series = row.isnull()
            if is_nan_series.any():
                rows_with_nan.append(index)

        df_final = df.drop(df.index[rows_with_nan[0]:100000000])

        df_final.columns = ['tarih',
                            'nufus_onbes_yukarı',
                            'isgucu',
                            'istihdam_edilen',
                            'issiz',
                            'isgucune_dahil_olmayan_nufus',
                            'isgucune_katilma_orani',
                            'issizlik_orani',
                            'tarim_disi_issizlik_orani',
                            'istihdam_orani']
        print(df_final)
        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)
        print(df_final)

        df_final.to_sql('nufusun_isgucu_durumu', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'nufus_onbes_yukarı': sqlalchemy.types.Numeric(),
                                                 'isgucu': sqlalchemy.types.Numeric(),
                                                 'istihdam_edilen': sqlalchemy.types.Numeric(),
                                                 'issiz': sqlalchemy.types.Numeric(),
                                                 'isgucune_dahil_olmayan_nufus': sqlalchemy.types.Numeric(),
                                                 'isgucune_katilma_orani': sqlalchemy.types.Numeric(),
                                                 'issizlik_orani': sqlalchemy.types.Numeric(),
                                                 'tarim_disi_issizlik_orani': sqlalchemy.types.Numeric(),
                                                 'istihdam_orani': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - nufusun_isgucu_durumu")

    def tcmb_politika_faizi(self):
        df_mn = pd.read_html(
            'https://www.tcmb.gov.tr/wps/wcm/connect/TR/TCMB+TR/Main+Menu/Temel+Faaliyetler/Para+Politikasi/Merkez+Bankasi+Faiz+Oranlari/1+Hafta+Repo')
        df_mn1 = df_mn[0]
        df_mn1.columns = df_mn1.iloc[0]
        df = df_mn1.iloc[pd.RangeIndex(len(df_mn1)).drop(0)]
        df.columns = ['tarih', 'borc_alma', 'borc_verme']
        df_final = df[['tarih', 'borc_verme']]
        df_final['tarih'] = df_final['tarih'].str.replace('.', '-')
        df_final['tarih'] = pd.to_datetime(df_final['tarih'])
        df_final.to_sql('tcmb_politika_faizi', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'tarih': sqlalchemy.types.DATE(),
                                                 'borc_verme': sqlalchemy.types.Numeric()})
        print("Database insert completed - tcmb_politika_faizi")

    def satin_alma_mudurleri_endeksi(self):
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        driver = webdriver.Chrome(r"chromedriver.exe", options=option)
        driver.get(
            "https://tr.investing.com/economic-calendar/turkish-manufacturing-pmi-1305")  # url that data will fetch from there
        time.sleep(5)

        for index in range(11):
            driver.find_element_by_xpath("//*[@id='showMoreHistory1305']/a").click()
            time.sleep(1)

        time.sleep(5)
        tbl = driver.find_element_by_xpath("//*[@id='eventHistoryTable1305']").get_attribute('outerHTML')
        df_mn = pd.read_html(tbl)
        df_mn1 = df_mn[0].dropna(axis=0, thresh=1)
        df_mn1.columns = ['yayinlanma_tarihi', 'zaman', 'aciklanan', 'beklenti', 'onceki', 'Unnamed: 5']
        df_final = df_mn1[['yayinlanma_tarihi', 'zaman', 'aciklanan', 'beklenti', 'onceki']]
        df_final.to_sql('satin_alma_mudurleri_endeksi', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'yayinlanma_tarihi': sqlalchemy.types.VARCHAR(length=20),
                                                 'zaman': sqlalchemy.types.VARCHAR(length=10),
                                                 'aciklanan': sqlalchemy.types.Float(),
                                                 'beklenti': sqlalchemy.types.Float(),
                                                 'onceki': sqlalchemy.types.Float()})
        driver.close()
        print("Database insert completed - satin_alma_mudurleri_endeksi")

    def update_satin_alma_mudurleri_endeksi(self):
        print("satin_alma_mudurleri_endeksi updating...")
        self.vertica_connection.execute(
            f"update satin_alma_mudurleri_endeksi set yayinlanma_tarihi = to_date(left(yayinlanma_tarihi, instr(yayinlanma_tarihi,' (')),'dd.mm.yyyy')")
        print("yayinlanma_tarihi column is updated!")
        self.vertica_connection.execute(
            f"ALTER TABLE satin_alma_mudurleri_endeksi ADD COLUMN yayinlanma_tarihi_as_date date DEFAULT yayinlanma_tarihi::date")
        print("satin_alma_mudurleri_endeksi is altered!")

    def foreign_currency_insert(self):
        file_path = "foreign_currency.xlsx"

        df = pd.read_excel(file_path)

        df_tarih = pd.DataFrame(df['Tarih'])

        rows_with_nan = []
        for index, row in df_tarih.iterrows():
            is_nan_series = row.isnull()
            if is_nan_series.any():
                rows_with_nan.append(index)

        df_mn = df.drop(df.index[rows_with_nan[0]:100000000])
        df_mn1 = df_mn[
            ['Tarih', 'TP DK USD A YTL', 'TP DK USD S YTL', 'TP DK EUR A YTL', 'TP DK EUR S YTL', 'TP DK GBP A YTL',
             'TP DK GBP S YTL']]
        df_mn1.columns = ['tarih',
                          '(USD) ABD Doları (Döviz Alış)',
                          '(USD) ABD Doları (Döviz Satış)',
                          '(EUR) Euro (Döviz Alış)',
                          '(EUR) Euro (Döviz Satış)',
                          '(GBP) İngiliz Sterlini (Döviz Alış)',
                          '(GBP) İngiliz Sterlini (Döviz Satış)']
        df_final = pd.melt(df_mn1, id_vars=['tarih'], value_vars=['(USD) ABD Doları (Döviz Alış)',
                                                                  '(USD) ABD Doları (Döviz Satış)',
                                                                  '(EUR) Euro (Döviz Alış)',
                                                                  '(EUR) Euro (Döviz Satış)',
                                                                  '(GBP) İngiliz Sterlini (Döviz Alış)',
                                                                  '(GBP) İngiliz Sterlini (Döviz Satış)'],
                           var_name='doviz', value_name='deger')
        print(df_final)

        df_final.to_sql('foreign_currency', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'tarih': sqlalchemy.types.VARCHAR(length=20),
                                                 'doviz': sqlalchemy.types.VARCHAR(length=100),
                                                 'deger': sqlalchemy.types.Numeric()})
        print("Database insert completed - foreign_currency")


def main():
    logging.basicConfig(filename='tim_fetch.log', level=logging.WARNING,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.warning("Started whole process")

    DataFetch()
    logging.info("Data fetched successfully...")
    print("****************************************************")
    transform_data = InsertData()
    write_start_time = time.time()
    logging.info("Database insert starting...")
    print("Database insert starting...")
    transform_data.gsyih_hasila_degisim_orani_insert()
    transform_data.update_gsyih_hacim_degisim_oran()
    transform_data.total_export_insert()
    transform_data.total_import_insert()
    transform_data.industrial_production_index_insert()
    transform_data.tufe_ufe_numbers_insert()
    transform_data.nufusun_isgucu_durumu_insert()
    transform_data.tcmb_politika_faizi()
    transform_data.satin_alma_mudurleri_endeksi()
    transform_data.update_satin_alma_mudurleri_endeksi()
    transform_data.foreign_currency_insert()
    write_end_time = time.time()
    print("Database insert completed with(time) : ", write_end_time - write_start_time)
    logging.warning("Database insert completed with(time) : " + str(write_end_time - write_start_time))


if __name__ == '__main__':
    main()
    logging.warning("All process completed. ")
    sys.exit()
