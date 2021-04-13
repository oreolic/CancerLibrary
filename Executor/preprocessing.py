import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from Executor import directional as da
from Executor import dataparsing as dp
from datetime import datetime


class preprocessing_raw_data:


    def __init__(self, data_type, replicate, day):
        self.data_type = data_type
        self.replicate = replicate
        self.day = day
        self.chunk = 25

        return

    def read_raw_data (self):
        t1 = datetime.now()  ##{}_{}{}_all_random_barcode.txt 24K_R1_D10_all_random_barcode
        df = pd.read_csv('Input/Random_Barcode_RAW_Data/{}_{}_{}_all_random_barcode.txt'
                                .format(self.data_type, self.replicate, self.day)
                                , delimiter='\t')  
        
            
        if df.shape[1] == 5:
            df.columns = ['Sorting_barcode', 'Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB',
                                'RandomBarcode', '{}_Count'.format(self.day)]

            df = df[['Sorting_barcode', 'RandomBarcode','{}_Count'.format(self.day),'Unique_RandomBarcodeNumber_In_SortingBarcode',
                    ' Total_reads_of_SB']]

            df = df.drop(columns=['Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB'])
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)
            return df
        
        elif df.shape[1] == 3:
            df.columns = ['Sorting_barcode', 'RandomBarcode', '{}_Count'.format(self.day)]
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)           
            
            return df

        elif df.shape[1] == 4:
            df.columns = ['Sorting_barcode', 'Unique_RandomBarcodeNumber_In_SortingBarcode',
                                'RandomBarcode', '{}_Count'.format(self.day)]

            df = df[['Sorting_barcode', 'RandomBarcode','{}_Count'.format(self.day),'Unique_RandomBarcodeNumber_In_SortingBarcode']]
            
            df = df.drop(columns=['Unique_RandomBarcodeNumber_In_SortingBarcode'])
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)
            return df
    
    



class combine_cont_treat:

    def _merge_control_treat(self, eachdf1, eachdf2):
        ## a: D10, b: D24 eachDF 
        eachdf1 = eachdf1.set_index('RandomBarcode')

        eachdf2 = eachdf2.set_index('RandomBarcode')
        eachdf2 = eachdf2.drop(columns='Sorting_barcode')

        df = pd.concat([eachdf1,eachdf2],axis=1).fillna(0)

        df = df[df['D10_Count']!=0]

        df = df.reset_index()
        df = df.rename(columns = {'index':'RandomBarcode'})

        df = df[['Sorting_barcode','RandomBarcode','D10_Count','D24_Count']]
        
        return df

    def multi_combinde_data(self,pre_proc_data):
        t1 = datetime.now()
        df1 = pre_proc_data[0]
        df2 = pre_proc_data[1]
        df1.columns = ['Sorting_barcode','RandomBarcode','D10_Count']

        dflst1 = dp.data_parsing().divide_df_into_list(df1)
        dflst2 = dp.data_parsing().divide_df_into_list(df2)        

        dic1 = {} ## Day 10 

        for eachdf in dflst1:
            sb = eachdf.iloc[0,0]
            dic1[sb] = eachdf

        dic2 = {} ## Day 24

        for eachdf2 in dflst2:
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
                    fut = executor.submit(self._merge_control_treat,eachdf1, eachdf2)
                    final[sb] = fut
        
        for sb in final:
            fut = final[sb]
            final[sb] = fut.result()
                   
        t2 = datetime.now()        
        print('Combine_Data: ', t2-t1)

        return final




def _directional_adjacency (dflst):
    d = da.directional()
    da_dflst = d.multi_directional_adjacency(dflst)
    return da_dflst


def _directionaladjacency(data_combination):

    tup = data_combination[0]
    
    ## Data_combination = [(2KABE,..)]
    ##tup = (2KABE,R1,D10,D24)
    cont = preprocessing_raw_data(tup[0], tup[1], tup[2]) ##D10)
    trea = preprocessing_raw_data(tup[0], tup[1], tup[3]) ## D24

    d10 = cont.read_raw_data()
    d24 = trea.read_raw_data()

    d10_lst = dp.data_parsing().divide_df_into_list(d10)   
    d24_lst = dp.data_parsing().divide_df_into_list(d24)

    print('Start Calculate Directional Adjacency')
    da_d10 = _directional_adjacency(d10_lst).rename(columns={0:'RandomBarcode',1:'{}_Count'.format(tup[2])})
    da_d24 = _directional_adjacency(d24_lst).rename(columns={0:'RandomBarcode',1:'{}_Count'.format(tup[3])})

    
    da_d10.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[2]), sep = '\t'
                            ,index=False)
    da_d24.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[3]), sep = '\t'
                            ,index=False)
    
    print('End Calculate Directional Adjacency')
    ## Output: (DA_D10_DF,DA_D24_DF)
    return da_d10,da_d24





def _combine_d10_d24(pre_pro_data):
    ## Input:  (D10_DFlist,D24_DFlist)
    cb = combine_cont_treat()
    dfdic = cb.multi_combinde_data(pre_pro_data)  
    lst = []
    for sb in dfdic:
        lst.append(dfdic[sb])
    final = pd.concat(lst)
    return final




def data_pre_processing(data_combination):  
    tup = data_combination[0]

    da_result = _directionaladjacency(data_combination)
    final = _combine_d10_d24(da_result) 
    print('DataProcessing Done')   
    
    
    final.columns = ['Sorting_barcode','RandomBarcode','{}_Count'.format(tup[2]),'{}_Count'.format(tup[3])]
    final.to_csv('Result/{}_{}_Processed_Count.txt'.format(data_combination[0][0], data_combination[0][1]),sep='\t',index=None)
    return 


