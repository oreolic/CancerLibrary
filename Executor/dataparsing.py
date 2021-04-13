#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime


class data_parsing:
    def _barcode_index (self,df):
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
    


    def divide_df_into_list (self,DF):
        bc_lst = self._barcode_index(DF)
        n = 0 
        t1 = datetime.now()
        n = 0
        lst = []
        while n < len(bc_lst)-1:
            tup0 = bc_lst[n]
            tup1 = bc_lst[n+1]
            
            idx = tup0[1]
            idx1 = tup1[1]

            ## tup = (BC, idx)

            eachdf = DF[idx:idx1]
            lst.append(eachdf)

            if n == len(bc_lst)-2:
                lastdf = DF[idx1:]              
                lst.append(lastdf)

            n += 1
        t2 = datetime.now()

        print('Make DF list:', t2-t1)
        return lst

