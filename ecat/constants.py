import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

format = '%(asctime)s %(message)s'
datefmt='%d %b %y %H:%M:%S'
logging.basicConfig(level=logging.INFO, format=format, datefmt=datefmt)


class STATUS():
    ''' Helper class to encapsulate e-Catalogue status values '''

    def __init__(self):

        self.status = {10257: 'NotForCatalog', 10258: 'PendingDataEntry',
                       10259: 'PendingApproval', 10260: 'Approved',
                       10261: 'Rejected', 10262: 'Withdrawn',
                       10263: 'PendingProductLaunch', 10264: 'Obsolete',
                       1000218: 'NotThisCompany'}

    def get(self, key:str) -> str:
        return self.status.get(key)

    def get_dataframe(self) -> pd.DataFrame:
        self.df = pd.DataFrame([self.status]).T.reset_index()
        self.df.columns = ['status', 'status_description']

        return self.df

class COMMON_COLS():
    ''' Columns column to both classroom and ecatalogue databases '''


    def __init__(self):

        self.common_cols = ['PRODUCTCODE_ID', 'CATALOG_ID', 'BAXTER_PRODUCTCODE',
                'UMDNS', 'ATC', 'CE_MARK_CODE', 'CE_MARK_CLASS',
                'MINIMUM_STORAGE_TEMPERATURE', 'MAXIMUM_STORAGE_TEMPERATURE',
                'MINIMUM_STORAGE_TEMPERATURE_UM', 'MAXIMUM_STORAGE_TEMPERATURE_UM',
                'PRODUCT_NAME', 'LONG_DESCRIPTION', 'TRADEMARK', 'MANUFACTURER',
                'VOLUME', 'VOLUME_UOM', 'CONCENTRATION', 'GAUGE', 'GAUGE_UOM',
                'LENGTH', 'LENGTH_UOM', 'INFUSION_DURATION', 'INFUSION_DURATION_UOM',
                'STERILISATION_METHOD', 'ARTERIAL_VENOUS', 'TACTILE_I_D', 'COLOR_CODE',
                'PD_DELIVERY_SYSTEM', 'CONNECTOLOGY', 'ANTI_REFLUX', 'DEHP_FREE',
                'PVC_FREE', 'LATEX_FREE', 'LATEX_FREE_COMMENT', 'LIPID_RESISTANT',
                'NEEDLE_PROTECTOR', 'DIALYSER_SURFACE_AREA', 'WASTE_DISPOSAL',
                'OVERPOUCH_REQUIRED', 'KEYWORDS', 'CSS_STATUS', 'GHX_STATUS',
                'REGULATORY_COMMENT', 'DATE_LASTMODIFIED', 'LAST_USER', 'SPC_URL',
                'DATE_APPROVED']

    def get(self) -> list:

        return self.common_cols
