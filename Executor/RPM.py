#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from Executor import DataParsing as DP


#%%
class TotalRPM:

    def read_data(self, data_info):
        t1 = datetime.now()
        df = pd.read_csv( ##{}_{}_Raw_Data_Integration_File
            'Result/{}_{}_Processed_Count.txt'.format(data_info[0][0], data_info[0][1])
            , delimiter='\t')
        t2 = datetime.now()
    
        print('Read_PDS', t2 - t1)
        return df


    def rpm(self,df,data_info):
        ###DF['D24_Count'] = DF['D24_Count']
        
        tup = data_info[0]

        d10 = tup[2]
        d24 = tup[3]
        tot10 = df['{}_Count'.format(d10)].sum()
        tot24 = df['{}_Count'.format(d24)].sum()

        rpm10 = lambda x:x/tot10*1e6
        rpm24 = lambda x:x/tot24*1e6

        df['{}_RPM'.format(d10)] = df['{}_Count'.format(d10)].apply(rpm10)
        df['{}_RPM'.format(d24)] = df['{}_Count'.format(d24)].apply(rpm24)

        return df




    def make_output(self, rpmdf, data_info):
        tup = data_info[0]
        rpmdf.to_csv('Result/{}_{}_totalRPM_FC_Result.txt'.format(data_info[0][0], data_info[0][1]),
                  sep='\t', index=False, 
                  header=['Sorting_Barcode', 'RandomBarcode', '{}_Count'.format(tup[2]), '{}_Count'.format(tup[3])
                                                 ,'{}_RPM'.format(tup[2]),'{}_RPM'.format(tup[3]), 'Fold_Change'])
                                                 
        return 



    def calculate_rpm(self,data_info):

        a = self.read_data(data_info) 
     
        t3 = datetime.now()
        c = self.rpm(a,data_info)

        t4 = datetime.now()
        print('RPM_calculation: ', t4-t3)
        self.make_output(c,data_info)

        return 


    
