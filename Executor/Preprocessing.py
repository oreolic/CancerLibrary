#%%
from numpy import e
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from Executor import Directional as DA
from Executor import DataParsing as DP
from datetime import datetime
import statistics
import pathlib
import os
import operator
import sys



#%%

class clsInput:

    def _Input_Data_Parsing(self):
        FileList = [sys.argv[i] for i in range(1, len(sys.argv))]

        ParsedData = []
        for eachFile in FileList:
            parsed = self._SplitingFileName(eachFile)
            ParsedData.append(parsed)

        return ParsedData



    def _SplitingFileName(self, FileName):
        FileInfo = FileName[:-23]
        Proj,Rep,Day = FileInfo.split('_')
        return Proj,Rep,Day



    def DataCombination(self):
        ParsedList = self._Input_Data_Parsing()

        DataComb = []
        n = 0
        while n < len(ParsedList):
            DataComb.append((ParsedList[n],ParsedList[n+1]))
            n += 2

        return DataComb


#%%
class PreProcessing_of_Raw_Data:
    """
 
    """

    def __init__(self, data_type, replicate, day):
        self.data_type = data_type
        self.replicate = replicate
        self.day = day
        self.chunk = 25

        return

    def Read_RAW_data(self):
        t1 = datetime.now()  ##{}_{}{}_all_random_barcode.txt 24K_R1_D10_all_random_barcode
        if self.data_type in ['84K','24K']:
            pds_data = pd.read_csv('Input/Random_Barcode_RAW_Data/{}_{}_{}_all_random_barcode.txt'
                                .format(self.data_type, self.replicate, self.day)
                                , delimiter='\t',index_col=0)
            if 'TTTGCACAGTCACTCATCGATCTCTACG' in list(pds_data.index):
                pds_data = pds_data.drop(index = 'TTTGCACAGTCACTCATCGATCTCTACG') 
                pds_data = pds_data.reset_index()
            else:
                pass
        else:
            pass 

        pds_data = pd.read_csv('Input/Random_Barcode_RAW_Data/{}_{}_{}_all_random_barcode.txt'
                                .format(self.data_type, self.replicate, self.day)
                                , delimiter='\t')  
        
            
        if pds_data.shape[1] == 5:
            pds_data.columns = ['Sorting_barcode', 'Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB',
                                'RandomBarcode', '{}_Count'.format(self.day)]

            PDS = pds_data[['Sorting_barcode', 'RandomBarcode','{}_Count'.format(self.day),'Unique_RandomBarcodeNumber_In_SortingBarcode',
                    ' Total_reads_of_SB']]

            PDS = PDS.drop(columns=['Unique_RandomBarcodeNumber_In_SortingBarcode', ' Total_reads_of_SB'])
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)
            return PDS
        
        elif pds_data.shape[1] == 3:
            pds_data.columns = ['Sorting_barcode', 'RandomBarcode', '{}_Count'.format(self.day)]
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)           
            
            return pds_data

        elif pds_data.shape[1] == 4:
            pds_data.columns = ['Sorting_barcode', 'Unique_RandomBarcodeNumber_In_SortingBarcode',
                                'RandomBarcode', '{}_Count'.format(self.day)]

            PDS = pds_data[['Sorting_barcode', 'RandomBarcode','{}_Count'.format(self.day),'Unique_RandomBarcodeNumber_In_SortingBarcode']]
            
            PDS = PDS.drop(columns=['Unique_RandomBarcodeNumber_In_SortingBarcode'])
            t2 = datetime.now()
            print('Read_file: ', t2 - t1)
            return PDS



    def DFlist (self, PDS):
        DFlist = DP.Data_Parsing().Divide_PDS_data(PDS)
        return DFlist


    
    
#%%
class clsCombine_Data:

    def _Merge_Control_Treat(self, eachdf1, eachdf2):
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

    def MultiCombineData(self,PreProcData):
        t1 = datetime.now()
        DF1 = PreProcData[0]
        DF2 = PreProcData[1]
        DF1.columns = ['Sorting_barcode','RandomBarcode','D10_Count']
        DFlist1 = DP.Data_Parsing().Divide_PDS_data(DF1)
        DFlist2 = DP.Data_Parsing().Divide_PDS_data(DF2)        

        dic1 = {} ## Day 10 

        for eachdf in DFlist1:
            sb = eachdf.iloc[0,0]
            dic1[sb] = eachdf

        dic2 = {} ## Day 24

        for eachdf2 in DFlist2:
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
                    fut = executor.submit(self._Merge_Control_Treat,eachdf1, eachdf2)
                    final[sb] = fut
        
        for sb in final:
            fut = final[sb]
            final[sb] = fut.result()
                   
        t2 = datetime.now()
        print('Combine_Data: ', t2-t1)

        return final




def _Directional_Adjacency(DFlist):
    D = DA.clsDirectional()
    DA_DFlist = D.Multi_Directional_Adjacency(DFlist)
    return DA_DFlist


def _DirectionalAdjacency(Data_combination):

    tup = Data_combination[0]
    
    Cont = PreProcessing_of_Raw_Data(tup[0], tup[1], tup[2]) ##D10)
    Trea = PreProcessing_of_Raw_Data(tup[0], tup[1], tup[3]) ## D24

    D10_PDS = Cont.Read_RAW_data()
    D24_PDS = Trea.Read_RAW_data()

    D10_DFlist = Cont.DFlist(D10_PDS)   
    D24_DFlist = Trea.DFlist(D24_PDS)

    print('Start Calculate Directional Adjacency')
    DA_D10_DF = _Directional_Adjacency(D10_DFlist).rename(columns={0:'RandomBarcode',1:'{}_Count'.format(tup[2])})
    DA_D24_DF = _Directional_Adjacency(D24_DFlist).rename(columns={0:'RandomBarcode',1:'{}_Count'.format(tup[3])})

    
    DA_D10_DF.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[2]), sep = '\t'
                            ,index=False)
    DA_D24_DF.to_csv('Result/{a}_{b}{c}_Directional_Adjacency.txt'.format(a=tup[0],b=tup[1],c=tup[3]), sep = '\t'
                            ,index=False)
    
    print('End Calculate Directional Adjacency')
## Output: (DA_D10_DF,DA_D24_DF)
    return DA_D10_DF,DA_D24_DF





def _Combine_D10_D24(PreProcessing_DATA):
    ## Input:  (D10_DFlist,D24_DFlist)
    CB = clsCombine_Data()
    DFdic = CB.MultiCombineData(PreProcessing_DATA)  
    LIST = []
    for sb in DFdic:
        LIST.append(DFdic[sb])
    final = pd.concat(LIST)
    return final




#%%

def DataPreProcessing(Data_combination):  
    tup = Data_combination[0]

    DAtup = _DirectionalAdjacency(Data_combination)
    finalDF = _Combine_D10_D24(DAtup) 
    print('DataProcessing Done')   
    
    finalDF.columns = ['Sorting_barcode','RandomBarcode','{}_Count'.format(tup[2]),'{}_Count'.format(tup[3])]
    finalDF.to_csv('Result/{}_{}_Processed_Count.txt'.format(Data_combination[0][0], Data_combination[0][1]),sep='\t',index=None)
    return 


# %%
