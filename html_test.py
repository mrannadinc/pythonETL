import pandas as pd

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
    # df_final.to_sql('tcmb_politika_faizi', con=self.vertica_connection, if_exists='append',
    #                 index=False,
    #                 index_label='id', dtype={'borc_verme': sqlalchemy.types.Numeric(),
    #                                          'yil': sqlalchemy.types.VARCHAR(length=5),
    #                                          'ay': sqlalchemy.types.VARCHAR(length=3)})
    print(df_final)
    print("Database insert completed - tcmb_politika_faizi")