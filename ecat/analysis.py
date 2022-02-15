import pandas as pd
import logging
from ecat.xl import write_excel
from ecat.constants import STATUS

logger = logging.getLogger(__name__)

def generate_analysis(df_classroom: pd.DataFrame, df_product: pd.DataFrame,
                      df_p_product: pd.DataFrame,
                      filename: str='outputs/ECAT_Classroom_Item_Analysis.xlsx') -> pd.DataFrame:
    '''
    Create a dataframe summarising the class_room item code, its 'status'
    and the corresponding information of whether the item exists in the
    product and p_product (published) eCatalogue data tables.

    Export results to an Excel WorkBook.

    Parameters
    ----------
    df_classroom
        'classroom' item dataframe (converted from CSV)
    df_product
        product data(frame) from eCatalogue
    df_p_product
        published product data(frame) from eCatalogue

    Returns
    -------
    Pandas DataFrame
    '''
    # Create merge/join dataframe(s) then merge into ONE.
    classroom_products = classroom_lookup(df_classroom, df_product)
    classroom_p_products = classroom_lookup(df_classroom, df_p_product, table_name='P_PRODUCT')

    classroom_merged_products = classroom_products.merge(classroom_p_products, how='left')

    # Add Baxter product code, product name, status and description.
    classroom_merged_products.insert(1, 'BAXTER_PRODUCTCODE', df_classroom.BAXTER_PRODUCTCODE)
    classroom_merged_products.insert(2, 'PRODUCT_NAME', df_classroom.PRODUCT_NAME)
    classroom_merged_products.insert(3, 'ARTICLE_STATUS', df_classroom.ARTICLE_STATUS)

    # Add status description
    s = STATUS()
    desc = classroom_merged_products['ARTICLE_STATUS'].replace(to_replace=s.status)
    classroom_merged_products.insert(4, 'STATUS_DESC', desc)

    # Generate analysis Excel WorkBook
    write_excel(classroom_merged_products, filename=filename)

    return classroom_merged_products


def classroom_lookup(df_classroom: pd.DataFrame, df_product: pd.DataFrame,
                     table_name: str='PRODUCT') -> pd.DataFrame:
    ''' Match/join classroom data with product/p_product tables (left_join)

    Lookup for a given classroom dataframe of items, whether the
    corresponding e-catalogue item(s) exist.

    Return a DataFrame of results

    Parameters
    ----------
    df_classroom
        'classroom' item dataframe (converted from CSV)
    df_product
        either the product or p_product (published) datatable
    table_name
        helper parameter to conveniently name the column
        either 'PRODUCT' or 'P_PRODUCT'

    Returns
    -------
    DataFrame containing merge/join information.
    '''

    df = df_classroom.merge(df_product, on='PRODUCTCODE_ID', how='left')

    df = (df.filter(regex='PRODUCTCODE_ID|CATALOG_ID_y')
            .rename(columns={'CATALOG_ID_y': table_name})
            .fillna(False))

    df.loc[df[table_name] != False, table_name] = True

    return df


def compare_data(df1: pd.DataFrame, df2: pd.DataFrame, df_classroom: pd.DataFrame,
                 table1: str='self', table2: str='other',
                 filename: str='outputs/ECAT_Compare.xlsx') -> pd.DataFrame:
    ''' Wrapper function for dataframe.compare()

    Compare classroom dataframe vs product / p_product data

    (Only works if both dataframes are the same size, that is:
    identical rows and columns.)

    df_classroom dataframe is also passed to this function to
    append product_id and baxter_productcode columns. This is to
    make it easier to identify product information.

    Export results to an Excel WorkBook.

    Parameters
    ----------
    df1
        first dataframe to compare
    df2
        second dataframe to compare
    df_classroom
        'classroom' item dataframe (converted from CSV)
    table1
        table1 label secondary heading label name
    table2
        table2 label secondary heading label name

    Returns
    -------
    Comparison pandas dataframe
    '''
    df_compare = df1.compare(df2, align_axis=0)
    df_compare = df_compare.reset_index().set_index('level_0')

    product_id = df_classroom['PRODUCTCODE_ID']
    df_compare.insert(0, 'PRODUCTCODE_ID', product_id)

    baxter_productcode = df_classroom['BAXTER_PRODUCTCODE']
    df_compare.insert(1, 'BAXTER_PRODUCTCODE', baxter_productcode)
    df_compare = df_compare.rename(columns={'level_1': 'TABLE_NAME'})

    replacements = {'self': table1.upper(), 'other':table2.upper()}
    df_compare.TABLE_NAME = df_compare.TABLE_NAME.replace(to_replace=replacements)

    write_excel(df_compare, filename=filename, freeze_panes=(1,3))

    return df_compare
