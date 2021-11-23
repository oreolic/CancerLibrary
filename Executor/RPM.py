#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from Executor import DataParsing as DP


#%%
class TotalRPM:

    def Read_PDS(self, Data_combination):
        t1 = datetime.now()
        PDS_data = pd.read_csv( ##{}_{}_Raw_Data_Integration_File
            'Result/{}_{}_Processed_Count.txt'.format(Data_combination[0][0], Data_combination[0][1])
            , delimiter='\t')
        t2 = datetime.now()
    
        print('Read_PDS', t2 - t1)
        return PDS_data


    def RPM(self,DF,Data_combination):
        ###DF['D24_Count'] = DF['D24_Count']
        
        tup = Data_combination[0]

        d10 = tup[2]
        d24 = tup[3]
        tot10 = DF['{}_Count'.format(d10)].sum()
        tot24 = DF['{}_Count'.format(d24)].sum()

        rpm10 = lambda x:x/tot10*1e6
        rpm24 = lambda x:x/tot24*1e6

        DF['{}_RPM'.format(d10)] = DF['{}_Count'.format(d10)].apply(rpm10)
        DF['{}_RPM'.format(d24)] = DF['{}_Count'.format(d24)].apply(rpm24)

        DF['Fold_Change'] = DF['{}_RPM'.format(d24)]/DF['{}_RPM'.format(d10)]

        return DF




    def Write_Pandas(self, rpmdf, Data_combination):
        tup = Data_combination[0]
        rpmdf.to_csv('Result/{}_{}_totalRPM_FC_Result.txt'.format(Data_combination[0][0], Data_combination[0][1]),
                  sep='\t', index=False, 
                  header=['Sorting_Barcode', 'RandomBarcode', '{}_Count'.format(tup[2]), '{}_Count'.format(tup[3])
                                                 ,'{}_RPM'.format(tup[2]),'{}_RPM'.format(tup[3]), 'Fold_Change'])
                                                 
        return 



    def Return_Bread(self,Data_combination):

        a = self.Read_PDS(Data_combination) 
     
        t3 = datetime.now()
        c = self.RPM(a,Data_combination)

        t4 = datetime.now()
        print('RPM_calculation: ', t4-t3)
        self.Write_Pandas(c,Data_combination)

        return 


    
