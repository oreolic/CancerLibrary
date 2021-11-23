#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime


class Data_Parsing:
    def dfdic(self,df):
        dflst = self.divide_df(df)
        dic = {}
        for eachdf in dflst:
            sb = eachdf.iloc[0,0]
            dic[sb] = eachdf
        return dic


    def _barcode_index(self,df):
        ##반드시 Sorting_Barocde가 column 0
        t1 = datetime.now()
        n = 1
        
        sb = list(df.iloc[:,0])
        lst = [(sb[0],0)]
        while n < df.shape[0]:
            if sb[n] != sb[n-1]:
                lst.append((sb[n],n))
            else:
                pass
            n+=1
        t2 = datetime.now()

        print('Barcode_Indexing:', t2-t1)
        return lst
    


    def divide_df(self,df):
        bclst = self._barcode_index(df)

        n = 0
        lst = []
        while n < len(bclst)-1:
            tup0 = bclst[n]
            tup1 = bclst[n+1]
            
            idx = tup0[1]
            idx1 = tup1[1]
            ## tup=(BC, idx)

            eachdf = df[idx:idx1]
            lst.append(eachdf)

            if n == len(bclst)-2:
                lastdf = df[idx1:]              
                lst.append(lastdf)

            n += 1
        return lst

