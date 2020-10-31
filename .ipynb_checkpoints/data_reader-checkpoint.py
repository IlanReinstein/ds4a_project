import os, sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob


def read_files(path):
    """
    Function to read all files from the 
    data directory. Returns lists for each type of
    data table (campaign, matrix, salesperson)
    """
    campaign_list = []
    matrix_list = []
    salespersons_list = []
    
    files = glob.glob(path)
    
    for f in files:
        if 'DATA CIERRE' in f:
            campaign_list.append(f)
        elif 'MATRIZ' in f:
            matrix_list.append(f)
        elif 'CLIENTES' in f:
            salespersons_list.append(f)
        else:
            print('file not relevant')
    return (campaign_list, matrix_list, salespersons_list)


def filter_campaign(campaign):
    """
    Function to filter campaign data to Moda Collection.
    """
    zero_demand = (campaign['dda_und'] == 0) & (campaign['fac_und'] != 0)
    return campaign[~zero_demand].copy()
    
    
def read_campaigns(file_list, viz):
    tables = [pd.read_excel(f) for f in file_list]
    return tables

def find_sheet_names(file_list):
    return [(f, pd.read_excel(f, None).keys()) for f in file_list]

def read_catalog(file_list):
    
    matrix_tables = []

    filt_cols = ['PLU', 'CLASIFICACIÓN', 'MES MODA', 'GÉNERO', 'MUNDO', '# PÁGINA', #'EXHIBICIÓN',
                 'ESTAMPADO P.INFERIORES', 'ESTAMPADO P.SUPERIORES', 'NUM_ APARICIONES',
                 'OCURRENCIA','PESO_ EXHIBICIÓN','PORTADA','ZOOM_PRODUCTO', 'EXPOSICIÓN']

    for f in file_list:
        print(f)
        try:
            print('Matriz')
            m = pd.read_excel(f, sheet_name='Matriz', usecols=filt_cols)
            print(m.columns)
        except:
            try:
                print('MATRIZ')
                m = pd.read_excel(f, sheet_name='MATRIZ ', usecols=filt_cols)
                print(m.columns)
            except:
                print('matriz')
                m = pd.read_excel(f, sheet_name='matriz', usecols=filt_cols)
                print(m.columns)

        matrix_tables.append(m)
        
    return pd.concat([table for table in matrix_tables])
    
def get_full_table(campaigns, catalog):
    full_merge = campaigns.merge(catalog, how = 'inner', on = ['PLU', 'CLASIFICACIÓN'])
    return full_merge
    
def save_csv(df, opath):
    df.to_csv(opath, index=None)
    print('file saved')
    
def to_pg(df, table_name, con):
        data = StringIO.StringIO()
        df.columns = cleanColumns(df.columns)
        df.to_csv(data, header=False, index=False)
        data.seek(0)
        raw = con.raw_connection()
        curs = raw.cursor()
        curs.execute("DROP TABLE " + table_name)
        empty_table = pd.io.sql.get_schema(df, table_name, con = con)
        empty_table = empty_table.replace('"', '')
        curs.execute(empty_table)
        curs.copy_from(data, table_name, sep = ',')
        curs.connection.commit()
    
    
if __name__ == "__main__":
    
    # Get list of files
    campaigns_list, matrix_list, _ = read_files('data/raw/*.xlsx')
    
    # Get All campaigns
    campaign_tables = read_campaigns(campaigns_list)
    full_campaigns = ppd.concat([campaign_tables])
    
    # Get catalog/matrix data
    catalogs = read_catalog(matrix_list)
    matriz8 = pd.read_excel('data/MATRIZ INFORMACIÓN CAMPAÑA 8-2020.xlsx', sheet_name='matriz')
    
    filt_cols8 = ['PLU', 'CLASIFICACIÓN', 'MES MODA', 'GÉNERO', 'MUNDO', '# PÁGINA', #'EXHIBICIÓN',
                 'ESTAMPADO P.INFERIORES', 'ESTAMPADO P.SUPERIORES', 'NUM_ APARICIONES',
                 'OCURRENCIA','PESO_ EXHIBICIÓN','PORTADA','ZOOM PRODUCTO', 'EXPOSICIÓN']

    catalog8 = matriz8[filt_cols8].rename(columns = {'ZOOM PRODUCTO': 'ZOOM_PRODUCTO'})
    full_catalog = pd.concat([catalogs, catalog8])
    
    
    save_csv(full_campaigns, 'data/output/pre_rds_tables/raw_campaign_data_1-8.csv')
    save_csv(full_catalog, 'data/output/pre_rds_tables/raw_catalog_data_1-8.csv')