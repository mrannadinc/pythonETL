import logging
import sqlalchemy

import sys
import time
from selenium import webdriver

import pandas as pd
from sqlalchemy import *
from sshtunnel import SSHTunnelForwarder


def vertica_connection():
    vertica_server = SSHTunnelForwarder(
        '*************',
        ssh_username="************",
        ssh_password="************",
        remote_bind_address=("************", 5434)
    )

    vertica_server.start()

    database_engine = create_engine("vertica+vertica_python://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('********',
                                                                                                       '**********',
                                                                                                       '***********',
                                                                                                       vertica_server.local_bind_port,
                                                                                                       '************'))

    return database_engine


def get_chrome_driver():
    option = webdriver.ChromeOptions()
    option.headless = True
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
        driver.find_element_by_xpath(f"//*[@id='serieMarketTableBody']/tr[{series_index}]/td[1]/label/span").click()
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
    button = driver.find_element_by_xpath("//*[@id='serieMarketTable']/div/div[2]/div[1]/div[3]/div/div/a")
    driver.execute_script("arguments[0].click();", button)
    time.sleep(10)


def create_report(driver):
    driver.find_element_by_xpath(
        "//*[@id='serieMarketGeneralOptions']/div/div[1]/div[2]/div[1]/button").click()
    time.sleep(3)


def clear_chosen_list(driver):
    driver.find_element_by_xpath(
        "//*[@id='shoppingCartList']/a").click()
    time.sleep(3)


def read_html(driver):
    create_report(driver)
    scr1 = driver.find_element_by_xpath('//*[@id="gridContainer"]/div/div[6]/div')
    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollWidth", scr1)
    time.sleep(10)
    table_MN = pd.read_html(driver.page_source)
    df = table_MN[2]

    clear_chosen_list(driver)
    time.sleep(3)
    return df


class DataFetch:
    def __init__(self):
        print("Starting Fetch: ")

    def gsyih_hacim_degisim_oran(self, driver):
        time.sleep(5)
        select_main_title(driver, 17)  # Üretim

        select_list_title(driver, 21, 6002)
        select_series(driver, 0, True)
        click_add_button(driver)
        df = read_html(driver)
        return df

    def industrial_production_index(self, driver):
        time.sleep(5)
        select_main_title(driver, 17)  # Üretim

        select_list_title(driver, 21, 5038)
        select_series(driver, 1, False)
        click_add_button(driver)
        df = read_html(driver)
        return df

    def total_export(self, driver):
        time.sleep(5)
        select_main_title(driver, 6)  # Dış ticaret istatistikleri

        list_title = [5118, 5985]
        series_list = [1, 2, 5, 14, 23]

        for index in list_title:
            select_list_title(driver, 19, index)
            if index == 5118:
                for series_index in series_list:
                    select_series(driver, series_index, False)
                click_add_button(driver)
            else:
                select_series(driver, 1, False)
                click_add_button(driver)
        df = read_html(driver)
        print(df)
        select_main_title(driver, 6)
        return df

    def total_import(self, driver):
        time.sleep(5)
        select_main_title(driver, 6)  # Dış ticaret istatistikleri

        list_title = [5119, 5990]
        series_list = [1, 2, 5, 14, 23]

        for index in list_title:
            select_list_title(driver, 19, index)
            if index == 5119:
                for series_index in series_list:
                    select_series(driver, series_index, False)
                click_add_button(driver)
            else:
                select_series(driver, 1, False)
                click_add_button(driver)
            click_add_button(driver)
        df = read_html(driver)
        print(df)
        return df

    def tufe_ufe_numbers(self, driver):
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
        df = read_html(driver)
        return df

    def nufusun_isgucu_durumu(self, driver):
        time.sleep(5)
        select_main_title(driver, 20)  # istihdam

        select_list_title(driver, 23, 6017)
        select_series(driver, 0, True)
        click_formula_button(driver)
        formula_list = [4, 1]
        for formula_index in formula_list:
            select_formula(driver, formula_index)
        click_add_button(driver)
        df = read_html(driver)
        return df

    def foreign_currency(self, driver):
        time.sleep(5)
        main_list = [1, 16]
        data_group = [2, 25]
        for main_index in main_list:
            select_main_title(driver, main_index)

            if 2 in data_group:
                select_list_title(driver, 2, 5863)
                select_series(driver, 0, True)
                click_add_button(driver)
                data_group.remove(2)
            else:
                select_list_title(driver, 25, 5849)
                driver.find_element_by_xpath(
                    f"//*[@id='serieMarketTableBody']/tr[3]/td[1]/label/span").click()
                time.sleep(3)
                click_add_button(driver)
        df = read_html(driver)
        return df

    def confidence_index(self, driver):
        time.sleep(5)
        select_main_title(driver, 12)  # anketler
        title_list = [5958, 5968]
        for index in title_list:
            select_list_title(driver, 15, index)
            select_series(driver, 1, False)
            click_add_button(driver)
        df = read_html(driver)
        return df


class InsertData:
    def __init__(self):
        self.vertica_connection = vertica_connection()
        self.drop_all_table()
        print("Selenium Chrome Driver Adding...")
        self.get_chrome_driver = get_chrome_driver()
        data_fetch = DataFetch()
        first_time = time.time()
        self.gsyih_hacim_degisim_oran = data_fetch.gsyih_hacim_degisim_oran(self.get_chrome_driver)
        second_time = time.time()
        print("--- %s fisrt take fetch first installing --- " % (second_time - first_time))
        third_time = time.time()
        self.total_export = data_fetch.total_export(self.get_chrome_driver)
        fourth_time = time.time()
        print("--- %s second take fetch first installing --- " % (fourth_time - third_time))
        fifth_time = time.time()
        self.total_import = data_fetch.total_import(self.get_chrome_driver)
        sixth_time = time.time()
        print("--- %s third take fetch first installing --- " % (sixth_time - fifth_time))
        seventh_time = time.time()
        self.industrial_production_index = data_fetch.industrial_production_index(self.get_chrome_driver)
        eight_time = time.time()
        print("--- %s fourth take fetch first installing --- " % (eight_time - seventh_time))
        ninth = time.time()
        self.tufe_ufe_numbers = data_fetch.tufe_ufe_numbers(self.get_chrome_driver)
        tenth = time.time()
        print("--- %s fifth take fetch first installing --- " % (tenth - ninth))
        eleventh = time.time()
        self.nufusun_isgucu_durumu = data_fetch.nufusun_isgucu_durumu(self.get_chrome_driver)
        twelfth = time.time()
        print("--- %s sixth take fetch first installing --- " % (twelfth - eleventh))
        thirteenth = time.time()
        self.foreign_currency = data_fetch.foreign_currency(self.get_chrome_driver)
        fourteenth = time.time()
        print("--- %s seventh take fetch first installing --- " % (fourteenth - thirteenth))
        fifteenth = time.time()
        self.confidence_index = data_fetch.confidence_index(self.get_chrome_driver)
        sixteenth = time.time()
        print("--- %s seventh take fetch first installing --- " % (sixteenth - fifteenth))
        self.get_chrome_driver.close()
        logging.info("Data fetched successfully...")

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
        df = self.gsyih_hacim_degisim_oran

        df.columns = ['yil',
                      'Yerleşik hanehalklarının tüketimi',
                      'Hanehalkına hizmet eden kar amacı olmayan kuruluşların tüketimi',
                      'Devletin nihai tüketim harcamaları',
                      'Gayrisafi sabit sermaye oluşumu',
                      'Mal ve hizmet ihracatı',
                      '(Eksi) Mal ve hizmet ithalatı',
                      'Gayrisafi yurtiçi hasıla']
        df_final = pd.melt(df, id_vars=['yil'], value_vars=['Yerleşik hanehalklarının tüketimi',
                                                            'Hanehalkına hizmet eden kar amacı olmayan kuruluşların tüketimi',
                                                            'Devletin nihai tüketim harcamaları',
                                                            'Gayrisafi sabit sermaye oluşumu',
                                                            'Mal ve hizmet ihracatı',
                                                            '(Eksi) Mal ve hizmet ithalatı',
                                                            'Gayrisafi yurtiçi hasıla'],
                           var_name='harcama_bilesenleri', value_name='hacim_bin_tl')
        df_final['hacim_bin_tl'] = 1000 * df_final['hacim_bin_tl']
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
        df = self.total_export

        df.columns = ['tarih',
                      'İhracat',
                      'Yatırım malları',
                      'Hammadde malları',
                      'Tüketim malları',
                      'Diğerleri',
                      'Birim deger endeksi']
        df_final = pd.melt(df, id_vars=['tarih'], value_vars=['İhracat',
                                                              'Yatırım malları',
                                                              'Hammadde malları',
                                                              'Tüketim malları',
                                                              'Diğerleri',
                                                              'Birim deger endeksi'],
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
        df = self.total_import

        df.columns = ['tarih',
                      'İhracat',
                      'Yatırım malları',
                      'Hammadde malları',
                      'Tüketim malları',
                      'Diğerleri',
                      'Birim deger endeksi']
        df_final = pd.melt(df, id_vars=['tarih'], value_vars=['İhracat',
                                                              'Yatırım malları',
                                                              'Hammadde malları',
                                                              'Tüketim malları',
                                                              'Diğerleri',
                                                              'Birim deger endeksi'],
                           var_name='siniflandirma', value_name='deger')

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

    def update_total_export_import(self):
        print("total_export_import updating...")
        self.vertica_connection.execute(
            "update total_export set deger = deger*1000 "
            "where siniflandirma in ('İhracat', 'Yatırım malları', 'Hammadde malları', 'Tüketim malları', 'Diğerleri')")
        print("deger is updated!")
        self.vertica_connection.execute(
            "update total_import set deger = deger*1000 "
            "where siniflandirma in ('İhracat', 'Yatırım malları', 'Hammadde malları', 'Tüketim malları', 'Diğerleri')")
        print("deger is updated!")

    def industrial_production_index_insert(self):
        df = self.industrial_production_index

        df.columns = ['tarih', 'toplam_sanayi_uretim_endeksi']
        df.dropna(inplace=True)
        df_mn1 = df['tarih'].str.split("-", n=1, expand=True)
        df["yil"] = df_mn1[0]
        df["ay"] = df_mn1[1]
        df.drop(columns=['tarih'], inplace=True)
        print(df)

        df.to_sql('industrial_production_index', con=self.vertica_connection, if_exists='append',
                  index=False,
                  index_label='id', dtype={'toplam_sanayi_uretim_endeksi': sqlalchemy.types.Numeric(),
                                           'yil': sqlalchemy.types.VARCHAR(length=5),
                                           'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - industrial_production_index")

    def tufe_ufe_numbers_insert(self):
        df_final = self.tufe_ufe_numbers

        df_final.columns = ['tarih', 'tufe', 'ufe']
        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)

        df_final.to_sql('tufe_ufe_numbers', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'tufe': sqlalchemy.types.Numeric(),
                                                 'ufe': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - tufe_ufe_numbers")

    def nufusun_isgucu_durumu_insert(self):
        df_final = self.nufusun_isgucu_durumu

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
        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)

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
        # df_final['tarih'] = df_final['tarih'].str.replace('.', '-')
        # df_final['tarih'] = pd.to_datetime(df_final['tarih'])
        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split(".", n=2, expand=True)
        df_final["yil"] = df_mn1[2]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)
        df_final.to_sql('tcmb_politika_faizi', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'borc_verme': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - tcmb_politika_faizi")

    def satin_alma_mudurleri_endeksi(self):
        option = webdriver.ChromeOptions()
        option.add_argument('headless')
        driver = webdriver.Chrome(r"C:chromedriver.exe", options=option)
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
            f"update satin_alma_mudurleri_endeksi set beklenti = beklenti/100, aciklanan = aciklanan/100, onceki = onceki/100")
        print("beklenti, aciklanan and onceki columns are updated!")
        self.vertica_connection.execute(
            f"ALTER TABLE satin_alma_mudurleri_endeksi ADD COLUMN yayinlanma_tarihi_as_date date DEFAULT yayinlanma_tarihi::date")
        print("satin_alma_mudurleri_endeksi is altered!")

    def foreign_currency_insert(self):
        df = self.foreign_currency

        df_mn1 = df[[0, 1, 2, 4, 5, 7, 47, 49]]
        df_mn1.columns = ['tarih',
                          '1 Ons Altın Londra Satış Fiyatı (ABD Doları/Ons)',
                          '(USD) ABD Doları (Döviz Alış)',
                          '(USD) ABD Doları (Döviz Satış)',
                          '(EUR) Euro (Döviz Alış)',
                          '(EUR) Euro (Döviz Satış)',
                          '(GBP) İngiliz Sterlini (Döviz Alış)',
                          '(GBP) İngiliz Sterlini (Döviz Satış)']
        df_final = pd.melt(df_mn1, id_vars=['tarih'], value_vars=['1 Ons Altın Londra Satış Fiyatı (ABD Doları/Ons)',
                                                                  '(USD) ABD Doları (Döviz Alış)',
                                                                  '(USD) ABD Doları (Döviz Satış)',
                                                                  '(EUR) Euro (Döviz Alış)',
                                                                  '(EUR) Euro (Döviz Satış)',
                                                                  '(GBP) İngiliz Sterlini (Döviz Alış)',
                                                                  '(GBP) İngiliz Sterlini (Döviz Satış)'],
                           var_name='seri', value_name='deger')
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)

        df_final.to_sql('foreign_currency', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'seri': sqlalchemy.types.VARCHAR(length=100),
                                                 'deger': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - foreign_currency")

    def confidence_index_insert(self):
        df_final = self.confidence_index

        df_final.columns = ['tarih', 'reel_kesim', 'tuketici']
        print(df_final)
        df_final.dropna(inplace=True)
        df_mn1 = df_final['tarih'].str.split("-", n=1, expand=True)
        df_final["yil"] = df_mn1[0]
        df_final["ay"] = df_mn1[1]
        df_final.drop(columns=['tarih'], inplace=True)
        print(df_final)

        df_final.to_sql('confidence_index', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'reel_kesim': sqlalchemy.types.Numeric(),
                                                 'tuketici': sqlalchemy.types.Numeric(),
                                                 'yil': sqlalchemy.types.VARCHAR(length=5),
                                                 'ay': sqlalchemy.types.VARCHAR(length=3)})
        print("Database insert completed - confidence_index")


def main():
    logging.basicConfig(filename='tim_fetch.log', level=logging.WARNING,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.warning("Started whole process")

    transform_data = InsertData()
    write_start_time = time.time()
    logging.info("Database insert starting...")
    print("Database insert starting...")
    transform_data.gsyih_hasila_degisim_orani_insert()
    transform_data.update_gsyih_hacim_degisim_oran()
    transform_data.total_export_insert()
    transform_data.total_import_insert()
    transform_data.update_total_export_import()
    transform_data.industrial_production_index_insert()
    transform_data.tufe_ufe_numbers_insert()
    transform_data.nufusun_isgucu_durumu_insert()
    transform_data.tcmb_politika_faizi()
    transform_data.satin_alma_mudurleri_endeksi()
    transform_data.update_satin_alma_mudurleri_endeksi()
    transform_data.foreign_currency_insert()
    transform_data.confidence_index_insert()
    write_end_time = time.time()
    print("Database insert completed with(time) : ", write_end_time - write_start_time)
    logging.warning("Database insert completed with(time) : " + str(write_end_time - write_start_time))


if __name__ == '__main__':
    main()
    logging.warning("All process completed. ")
    sys.exit()
