#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime


class Data_Parsing:
    def __init__(self):
        self.chunk = 25

    def dfdic(self,df):
        dflst = self.Divide_PDS_data(df)
        dic = {}
        for eachdf in dflst:
            sb = eachdf.iloc[0,0]
            dic[sb] = eachdf
        return dic

        
    def Divide_Dictionary(self, dic, N):
        List = []
        tup = [(i,dic[i]) for i in dic]

        for i in range(N):
            begin = (len(tup)//N)*i
            end = (len(tup) // N) * (i + 1)

            if i != N-1:
                List.append(tup[begin:end])
            else:
                List.append(tup[begin:])

        DicList = []


        for k in List:
            DicList.append(dict(k))
        return DicList

    def _Barcode_Index(self,DF):
        ##반드시 Sorting_Barocde가 column 0
        t1 = datetime.now()
        n = 1
        
        SB = list(DF.iloc[:,0])
        LIST = [(SB[0],0)]
        while n < DF.shape[0]:
            if SB[n] != SB[n-1]:
                LIST.append((SB[n],n))
            else:
                pass
            n+=1
        t2 = datetime.now()

        print('Barcode_Indexing:', t2-t1)
        return LIST
    


    def Divide_PDS_data(self,DF):
        BClist = self._Barcode_Index(DF)
        n = 0 
        t1 = datetime.now()
        n = 0
        LIST = []
        while n < len(BClist)-1:
            tup0 = BClist[n]
            tup1 = BClist[n+1]
            
            idx = tup0[1]
            idx1 = tup1[1]

            ## tup = (BC, idx)

            eachdf = DF[idx:idx1]
            LIST.append(eachdf)

            if n == len(BClist)-2:
                lastdf = DF[idx1:]              
                LIST.append(lastdf)

            n += 1
        t2 = datetime.now()

        print('Make DF list:', t2-t1)
        return LIST

# %%
class ReplicateMerge:
    def RPM_Merging(self,Data_combination):
        r1 = pd.read_csv('Result/{}_R1_totalRPM_FC_Result.txt'.format(Data_combination[0][0]),sep='\t')
        r2 = pd.read_csv('Result/{}_R2_totalRPM_FC_Result.txt'.format(Data_combination[0][0]),sep='\t')

        df = pd.concat([r1,r2])
        df = df.sort_values(by='Sorting_Barcode')

        df.to_csv('Result/{}_Merged_totalRPM_FC_Result.txt'.format(Data_combination[0][0]),sep='\t',index=None)
        return

