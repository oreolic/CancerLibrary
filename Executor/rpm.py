#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from Executor import dataparsing as dp


#%%
class total_rpm:

    def read_pds(self, data_combination):
        t1 = datetime.now()
        df = pd.read_csv( ##{}_{}_Raw_Data_Integration_File
            'Result/{}_{}_Processed_Count.txt'.format(data_combination[0][0], data_combination[0][1])
            , delimiter='\t')
        t2 = datetime.now()
    
        print('Read_PDS', t2 - t1)
        return df


    def rpm(self,df,data_combination):
  
        tup = data_combination[0]

        d10 = tup[2]
        d24 = tup[3]
        tot10 = df['{}_Count'.format(d10)].sum()
        tot24 = df['{}_Count'.format(d24)].sum()

        rpm10 = lambda x:x/tot10*1e6
        rpm24 = lambda x:x/tot24*1e6

        df.loc[:,'{}_RPM'.format(d10)] = df.loc[:,'{}_Count'.format(d10)].apply(rpm10)
        df.loc[:,'{}_RPM'.format(d24)] = df.loc[:,'{}_Count'.format(d24)].apply(rpm24)

        df['Fold_Change'] = df['{}_RPM'.format(d24)]/df['{}_RPM'.format(d10)]

        return df




    def write_df(self, rpmdf, data_combination):
        tup = data_combination[0]
        rpmdf.to_csv('Result/{}_{}_totalRPM_FC_Result.txt'.format(data_combination[0][0], data_combination[0][1]),
                  sep='\t', index=False, 
                  header=['Sorting_Barcode', 'RandomBarcode', '{}_Count'.format(tup[2]), '{}_Count'.format(tup[3])
                                                 ,'{}_RPM'.format(tup[2]),'{}_RPM'.format(tup[3]), 'Fold_Change'])
                                                 
        return 



    def rpm_excutor(self,data_combination):

        a = self.read_pds(data_combination) 
     
        t3 = datetime.now()
        c = self.rpm(a,data_combination)

        t4 = datetime.now()
        print('RPM_calculation: ', t4-t3)
        self.write_df(c,data_combination)
        return 


    
