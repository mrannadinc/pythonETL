import sqlalchemy

import sys
import time
from datetime import timedelta, datetime
from selenium import webdriver

import pandas as pd
import vertica_python
from sqlalchemy import *
from sshtunnel import SSHTunnelForwarder

DATE = (datetime.today() - timedelta(days=1)).strftime('%Y%m%d')


class Transform:
    def __init__(self):
        print("Starting: ")
        self.vertica_connection = self.vertica_connection()
        # self.vertica_local_connection = self.vertica_local_connection()

    def vertica_connection(self):
        vertica_server = SSHTunnelForwarder(
            '********',
            ssh_username="******",
            ssh_password="********",
            remote_bind_address=("**********", 5434)
        )

        vertica_server.start()

        engine = create_engine("vertica+vertica_python://{0}:{1}@{2}:{3}/{4}?charset=utf8".format('*******',
                                                                                                  '*******',
                                                                                                  '********',
                                                                                                  vertica_server.local_bind_port,
                                                                                                  '*********'))

        return engine

    def vertica_local_connection(self):
        conn_info = {'host': '**********',
                     'port': 5433,
                     'user': "*********",
                     'password': "*******"}
        connection = vertica_python.connect(**conn_info)
        return connection

    def gsyh_hacim_degisim_oranlari(self):
        file_name = "path" \
                    "Gayrisafi yurtiçi hasıla, harcama yöntemiyle zincirlenmiş hacim endeksi ve değişim oranları (2009=100).xls"
        df = pd.read_excel(file_name)
        # query = 'SELECT * from tim.gsyh_hacim_degisim_oranlari'
        # df = pd.read_sql_query(query, self.vertica_local_connection)
        # chunk["date_imported"] = pd.to_datetime('today')
        df.to_sql('gsyh_hacim_degisim_oranlari', con=self.vertica_connection, if_exists='append',
                  index=False,
                  index_label='id', dtype={'id': sqlalchemy.types.Integer(),
                                           'harcama_bilesenleri': sqlalchemy.types.VARCHAR(length=200),
                                           'yil': sqlalchemy.types.Integer(),
                                           'hacim_bin_tl': sqlalchemy.types.Numeric(),
                                           'endeks': sqlalchemy.types.Numeric(),
                                           'onceki_yilin_ceyregine_gore_degisim': sqlalchemy.types.Numeric(),
                                           'ceyrek': sqlalchemy.types.Integer()})
        print("Database insert completed - gsyh_hacim_degisim_oranlari")

    def aylara_gore_dis_ticaret(self):
        file_name = "Aylara Göre Dış Ticaret.xls"
        df = pd.read_excel(file_name)
        # chunk["date_imported"] = pd.to_datetime('today')
        df.to_sql('aylara_gore_dis_ticaret', con=self.vertica_connection, if_exists='append',
                  index=False,
                  index_label='id', dtype={'yil': sqlalchemy.types.Integer(),
                                           'ay': sqlalchemy.types.Integer(),
                                           'ihracat_FOB': sqlalchemy.types.Numeric(),
                                           'ithalat_CIF': sqlalchemy.types.Numeric(),
                                           'dis_ticaret_dengesi': sqlalchemy.types.Numeric(),
                                           'dis_ticaret_hacmi': sqlalchemy.types.Numeric(),
                                           'karsilama_orani': sqlalchemy.types.Numeric()})
        print("Database insert completed - aylara_gore_dis_ticaret")

    def yillara_gore_dis_ticaret(self):
        file_name = "Yıllara Göre Dış Ticaret.xls"
        df = pd.read_excel(file_name)
        # chunk["date_imported"] = pd.to_datetime('today')
        df.to_sql('yillara_gore_dis_ticaret', con=self.vertica_connection, if_exists='append',
                  index=False,
                  index_label='id', dtype={'yil': sqlalchemy.types.Integer(),
                                           'ihracat': sqlalchemy.types.Numeric(),
                                           'ithalat': sqlalchemy.types.Numeric(),
                                           'dis_ticaret_dengesi': sqlalchemy.types.Numeric(),
                                           'dis_ticaret_hacmi': sqlalchemy.types.Numeric(),
                                           'ihracatin_ithalati_karsilama_orani': sqlalchemy.types.Numeric()})
        print("Database insert completed - yillara_gore_dis_ticaret")

    def sitc_gore_ihracat(self):
        file_name = "Uluslararası Standart Ticaret Sınıflamasına Göre İhracat.xls"
        df = pd.read_excel(file_name)
        df_final = pd.melt(df, id_vars=['yil', 'sitc_rev4'],
                           value_vars=['ocak', 'subat', 'mart', 'nisan', 'mayis', 'haziran',
                                       'temmuz', 'agustos', 'eylul', 'ekim', 'kasim', 'aralik'],
                           var_name='ay', value_name='ihracat_degeri')
        # chunk["date_imported"] = pd.to_datetime('today')
        df_final.to_sql('sitc_gore_ihracat', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'yil': sqlalchemy.types.Integer(),
                                                 'sitc_rev4': sqlalchemy.types.VARCHAR(length=500),
                                                 'ay': sqlalchemy.types.VARCHAR(length=20),
                                                 'ihracat_degeri': sqlalchemy.types.Numeric()})
        print("Database insert completed - sitc_gore_ihracat")

    def sitc_gore_ithalat(self):
        file_name = "Uluslararası Standart Ticaret Sınıflamasına Göre İthalat.xls"
        df = pd.read_excel(file_name)
        df_final = pd.melt(df, id_vars=['yil', 'sitc_rev4'],
                           value_vars=['ocak', 'subat', 'mart', 'nisan', 'mayis', 'haziran',
                                       'temmuz', 'agustos', 'eylul', 'ekim', 'kasim', 'aralik'],
                           var_name='ay', value_name='ithalat_degeri')
        df_final.to_sql('sitc_gore_ithalat', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'yil': sqlalchemy.types.Integer(),
                                                 'sitc_rev4': sqlalchemy.types.VARCHAR(length=500),
                                                 'ay': sqlalchemy.types.VARCHAR(length=20),
                                                 'ithalat_degeri': sqlalchemy.types.Numeric()})
        print("Database insert completed - sitc_gore_ihracat")

    def tea_sanayi_uretim_endeksi(self):
        file_name = "Takvim Etkisinden Arındırılmış Sanayi Üretim Endeksi (2015=100).xls"
        df = pd.read_excel(file_name)
        df_final = pd.melt(df, id_vars=['nace_rev2', 'yil'],
                           value_vars=['ocak', 'subat', 'mart', 'nisan', 'mayis', 'haziran',
                                       'temmuz', 'agustos', 'eylul', 'ekim', 'kasim', 'aralik'],
                           var_name='ay', value_name='uretim_endeksi')
        df_final.to_sql('tea_sanayi_uretim_endeksi', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'nace_rev2': sqlalchemy.types.VARCHAR(length=50),
                                                 'yil': sqlalchemy.types.Integer(),
                                                 'ay': sqlalchemy.types.VARCHAR(length=20),
                                                 'uretim_endeksi': sqlalchemy.types.Numeric()})
        print("Database insert completed - tea_sanayi_uretim_endeksi")

    def tuketici_fiyat_endeksi(self):
        file_name = "Tüketici Fiyat Endeksi (2003=100).xls"
        df = pd.read_excel(file_name)
        df_final = pd.melt(df, id_vars=['yil'], value_vars=['ocak', 'subat', 'mart', 'nisan', 'mayis', 'haziran',
                                                            'temmuz', 'agustos', 'eylul', 'ekim', 'kasim', 'aralik'],
                           var_name='ay', value_name='tuketici_fiyat_endeksi')
        df_final.to_sql('tuketici_fiyat_endeksi', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'yil': sqlalchemy.types.Integer(),
                                                 'ay': sqlalchemy.types.VARCHAR(length=20),
                                                 'tuketici_fiyat_endeksi': sqlalchemy.types.Numeric()})
        print("Database insert completed - tuketici_fiyat_endeksi")

    def yurtici_uretici_fiyat_endeksi(self):
        file_name = "Yurt İçi Üretici Fiyat Endeksi (2003=100).xls"
        df = pd.read_excel(file_name)
        df_final = pd.melt(df, id_vars=['yil'], value_vars=['ocak', 'subat', 'mart', 'nisan', 'mayis', 'haziran',
                                                            'temmuz', 'agustos', 'eylul', 'ekim', 'kasim', 'aralik'],
                           var_name='ay', value_name='uretici_fiyat_endeksi')
        df_final.to_sql('yurtici_uretici_fiyat_endeksi', con=self.vertica_connection, if_exists='append',
                        index=False,
                        index_label='id', dtype={'yil': sqlalchemy.types.Integer(),
                                                 'ay': sqlalchemy.types.VARCHAR(length=20),
                                                 'tuketici_fiyat_endeksi': sqlalchemy.types.Numeric()})
        print("Database insert completed - yurtici_uretici_fiyat_endeksi")

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

    def ekonomik_guven_endeksi(self):
        file_name = "Ekonomik Güven Endeksi.xls"
        df = pd.read_excel(file_name)
        # chunk["date_imported"] = pd.to_datetime('today')
        df.to_sql('ekonomik_guven_endeksi', con=self.vertica_connection, if_exists='append',
                  index=False,
                  index_label='id', dtype={'yil': sqlalchemy.types.Integer(),
                                           'ay': sqlalchemy.types.Integer(),
                                           'ekonomik_guven_endeksi': sqlalchemy.types.Numeric(),
                                           'tuketici_guven_endeksi': sqlalchemy.types.Numeric()})
        print("Database insert completed - ekonomik_guven_endeksi")

    def satin_alma_mudurleri_endeksi(self):
        driver = webdriver.Chrome(r"chromedriver.exe")  # chromedriver path
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
                                                 'aciklanan': sqlalchemy.types.Numeric(),
                                                 'beklenti': sqlalchemy.types.Numeric(),
                                                 'onceki': sqlalchemy.types.Numeric()})
        print("Database insert completed - satin_alma_mudurleri_endeksi")

    def satin_alma_mudurleri_endeksi_alter(self):
        print("satin_alma_mudurleri_endeksi updating...")
        self.vertica_connection.execute(
            f"update satin_alma_mudurleri_endeksi set yayinlanma_tarihi = to_date(left(yayinlanma_tarihi, instr(yayinlanma_tarihi,' (')),'dd.mm.yyyy')")
        print("yayinlanma_tarihi column is updated!")
        self.vertica_connection.execute(
            f"ALTER TABLE satin_alma_mudurleri_endeksi ADD COLUMN yayinlanma_tarihi_as_date date DEFAULT yayinlanma_tarihi::date")
        self.vertica_connection.execute(
            f"ALTER TABLE satin_alma_mudurleri_endeksi ALTER COLUMN yayinlanma_tarihi_as_date DROP DEFAULT")
        self.vertica_connection.execute(
            f"ALTER TABLE satin_alma_mudurleri_endeksi DROP COLUMN yayinlanma_tarihi cascade")
        self.vertica_connection.execute(
            f"ALTER TABLE satin_alma_mudurleri_endeksi RENAME COLUMN yayinlanma_tarihi_as_date TO yayinlanma_tarihi")
        print("satin_alma_mudurleri_endeksi is altered!")


def main():
    transform_data = Transform()
    write_start_time = time.time()
    # transform_data.gsyh_hacim_degisim_oranlari()
    # transform_data.aylara_gore_dis_ticaret()
    # transform_data.yillara_gore_dis_ticaret()
    transform_data.sitc_gore_ihracat()
    transform_data.sitc_gore_ithalat()
    transform_data.tea_sanayi_uretim_endeksi()
    transform_data.tuketici_fiyat_endeksi()
    transform_data.yurtici_uretici_fiyat_endeksi()
    # transform_data.tcmb_politika_faizi()
    # transform_data.ekonomik_guven_endeksi()
    # transform_data.satin_alma_mudurleri_endeksi()
    # transform_data.satin_alma_mudurleri_endeksi_alter()
    write_end_time = time.time()
    print("Database insert completed with(time) : ", write_end_time - write_start_time)


if __name__ == '__main__':
    main()
    sys.exit()
