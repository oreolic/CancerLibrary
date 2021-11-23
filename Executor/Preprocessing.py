#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from Executor import Directional as DA
from Executor import DataParsing as DP
from datetime import datetime
import sys



#%%

class clsInput:

    def _input_data_parsing(self):
        files = [sys.argv[i] for i in range(1, len(sys.argv))]

        lst = []
        for file in files:
            parsed = self._split_filename(file)
            lst.append(parsed)

        return lst



    def _split_filename(self, filename):
        filename = filename[:-23]
        proj,rep,day = filename.split('_')
        return proj,rep,day



    def data_combi(self):
        parser_lst = self._input_data_parsing()

        lst = []
        n = 0
        while n < len(parser_lst):
            lst.append((parser_lst[n],parser_lst[n+1]))
            n += 2

        return lst


#%%
class PreProcessing_of_Raw_Data:

    def __init__(self, data_type, replicate, day):
        self.data_type = data_type
        self.replicate = replicate
        self.day = day
        self.chunk = 25

        return

    def read_data(self):
        t1 = datetime.now()  ##{}_{}{}_all_random_barcode.txt 24K_R1_D10_all_random_barcode
        df = pd.read_csv('Input/Random_Barcode_RAW_Data/{}_{}_{}_all_random_barcode.txt'
                                .format(self.data_type, self.replicate, self.day)
                                , delimiter='\t')  
        
            

        df.columns = ['Sorting_barcode', 'Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB',
                            'RandomBarcode', '{}_Count'.format(self.day)]

        df = df[['Sorting_barcode', 'RandomBarcode','{}_Count'.format(self.day),'Unique_RandomBarcodeNumber_In_SortingBarcode',
                ' Total_reads_of_SB']]

        df = df.drop(columns=['Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB'])
        t2 = datetime.now()
        print('Read_file: ', t2 - t1)
        return df
        


    def dflst (self, df):
        dflst = DP.Data_Parsing().divide_df(df)
        return dflst


    
    
#%%
class clsCombine_Data:

    def _merge_cont_treat(self, eachdf1, eachdf2):
        ## a: D10, b: D24 eachDF 

        eachdf1.columns = ['Sorting_barcode','RandomBarcode','D10_Count']
        eachdf2.columns = ['Sorting_barcode','RandomBarcode','D24_Count']

        eachdf1 = eachdf1.set_index('RandomBarcode')        
        eachdf2 = eachdf2.set_index('RandomBarcode')

        eachdf2 = eachdf2.drop(columns='Sorting_barcode')

        df = pd.concat([eachdf1,eachdf2],axis=1).fillna(0)

        df = df[df['D10_Count']!=0]

        df = df.reset_index()
        df = df.rename(columns = {'index':'RandomBarcode'})

        df = df[['Sorting_barcode','RandomBarcode','D10_Count','D24_Count']]
        
        return df

    def multi_conbine(self,pre_processed):
        t1 = datetime.now()
        df1 = pre_processed[0]
        df2 = pre_processed[1]
        df1.columns = ['Sorting_barcode','RandomBarcode','D10_Count']
        df1 = DP.Data_Parsing().divide_df(df1)
        df2 = DP.Data_Parsing().divide_df(df2)        

        dic1 = {} ## Day 10 

        for eachdf in df1:
            sb = eachdf.iloc[0,0]
            dic1[sb] = eachdf

        dic2 = {} ## Day 24

        for eachdf2 in df2:
            sb2 = eachdf2.iloc[0,0]
            dic2[sb2] = eachdf2

        final = {}
        with ProcessPoolExecutor(max_workers=20) as executor:            
            for sb in dic1:
                if sb not in dic2:  
                    pass
                else:
                    eachdf1 = dic1[sb]
                    eachdf2 = dic2[sb]
                    fut = executor.submit(self._merge_cont_treat,eachdf1, eachdf2)
                    final[sb] = fut
        
        for sb in final:
            fut = final[sb]
            final[sb] = fut.result()
                   
        t2 = datetime.now()
        print('Combine_Data: ', t2-t1)

        return final




def _directional(dflst):
    d = DA.clsDirectional()
    dflst = d.multi_directional(dflst)
    return dflst


def _directional_executor(data_info):

    tup = data_info[0]
    
    cont = PreProcessing_of_Raw_Data(tup[0], tup[1], tup[2]) ##D10)
    trea = PreProcessing_of_Raw_Data(tup[0], tup[1], tup[3]) ## D24

    d10 = cont.read_data()
    d24 = trea.read_data()

    d10 = cont.dflst(d10)   
    d24 = trea.dflst(d24)

    print('Start Calculate Directional Adjacency')
    d10 = _directional(d10).rename(columns={0:'RandomBarcode',1:'{}_Count'.format(tup[2])})
    d24 = _directional(d24).rename(columns={0:'RandomBarcode',1:'{}_Count'.format(tup[3])})

    
    d10.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[2]), sep = '\t'
                            ,index=False)
    d24.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[3]), sep = '\t'
                            ,index=False)
    
    print('End Calculate Directional Adjacency')
## Output: (DA_D10_DF,DA_D24_DF)
    return d10,d24





def _merge_d10_d24(preprocessed_data):
    ## Input:  (D10_DFlist,D24_DFlist)
    cb = clsCombine_Data()
    dfdic = cb.multi_conbine(preprocessed_data)  
    lst = []
    for sb in dfdic:
        lst.append(dfdic[sb])
    final = pd.concat(lst)
    return final




#%%

def data_preprocessing(data_info):  
    tup = data_info[0]

    da_tup = _directional_executor(data_info)
    finalDF = _merge_d10_d24(da_tup) 
    print('DataProcessing Done')   
    
    finalDF.columns = ['Sorting_barcode','RandomBarcode','{}_Count'.format(tup[2]),'{}_Count'.format(tup[3])]
    finalDF.to_csv('Result/{}_{}_Processed_Count.txt'.format(data_info[0][0], data_info[0][1]),sep='\t',index=None)
    return 


# %%
