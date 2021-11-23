#%%
import pandas as pd
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from Executor import DataParsing as DP

'''
Multi_divide -> Reindexing
'''

# %%
class RPM:
    def nonTarget(self, Data_combination):
        nt = []
        with open('Input/NonTarget/{}_nontarget.txt'.format(Data_combination[0][0]), 'r') as ntb:
            for a in ntb.readlines():
                nt.append(a.rstrip())
        return nt

    def Count_DF(self,Data_combination):
        DF = pd.read_csv('Result/{}_{}_totalRPM_FC_Result.txt'.format(Data_combination[0][0], Data_combination[0][1]), sep='\t')
        DF = DF.drop(columns=['D10_Count', 'D24_Count','Fold_Change'])
        
        
        
        return DF




    def _Divide_DFNT(self,DFlist,NT):
        t1 = datetime.now()
        df = pd.concat(DFlist)

        NTDFlist = []
        TGDFlist = []

        DF_BClist = list(set(df['Sorting_Barcode']))    
        DFmult    = df.set_index(['Sorting_Barcode','RandomBarcode'])

        for BC in DF_BClist:
            if BC in NT:
                eachDF = DFmult.loc[BC]
                eachDF = eachDF.reset_index()
                eachDF = eachDF.drop(['RandomBarcode'],axis= 1)
                eachDF['Sorting_Barcode'] = BC
                NTDFlist.append(eachDF)
            else:
                eachDF = DFmult.loc[BC]
                eachDF = eachDF.reset_index()
                eachDF = eachDF.drop(['RandomBarcode'],axis= 1)
                eachDF['Sorting_Barcode'] = BC
                TGDFlist.append(eachDF)

        t2 = datetime.now()
        
        result = (NTDFlist,TGDFlist)    
        print('Divide_NonTarget: ', t2-t1)
        return result
        





    def _Multi_Divide_DFNT(self,DF,NT):
        DFlist = DP.Data_Parsing().Divide_PDS_data(DF)
        
        with ProcessPoolExecutor(max_workers=25) as executor:
            futs = []

            for i in range(25):
                begin_idx = (len(DFlist) // 25) * i
                end_idx = (len(DFlist) // 25) * (i + 1)

                if i != 25 - 1:
                    fut = executor.submit(self._Divide_DFNT, DFlist[begin_idx:end_idx],NT)
                    futs.append(fut)
                else:
                    fut = executor.submit(self._Divide_DFNT, DFlist[begin_idx:],NT)
                    futs.append(fut)
            
            NTlist = []
            TGlist = []
            for fut in futs:
                tup = fut.result()
                NTlist.extend(tup[0])
                TGlist.extend(tup[1])
            
            return NTlist,TGlist
            
            

    def _Re_indexing(self,DivideDF,Data_combination):

        NTdf = pd.concat(DivideDF[0])
        TGdf = pd.concat(DivideDF[1])

        NTidx = ['NT{}'.format(i) for i in range(NTdf.shape[0])]
        TGidx = ['Rnd{}'.format(i) for i in range(TGdf.shape[0])]

        NTdf['sgRNA'] = NTidx
        TGdf['sgRNA'] = TGidx

        NTdf = NTdf[['sgRNA','Sorting_Barcode','D10_RPM','D24_RPM']]
        TGdf = TGdf[['sgRNA','Sorting_Barcode','D10_RPM','D24_RPM']]

        FinalDF = pd.concat([NTdf,TGdf])
        tup = Data_combination[0]

        NTfinal = pd.DataFrame(NTidx)
        
        NTfinal.to_csv('Mageck/{}_{}_Total_RPM_nonTarget.txt'.format(tup[0],tup[1]),header=None, index=None)
        FinalDF.to_csv('Mageck/{}_{}_Total_RPM_Mageck_Input.txt'.format(tup[0],tup[1]),sep='\t', index=None)
        return 


    def MageckConverter(self,Data_combination):
        DF = self.Count_DF(Data_combination)
        NT = self.nonTarget(Data_combination)

        DivideDF = self._Multi_Divide_DFNT(DF,NT)
        self._Re_indexing(DivideDF,Data_combination)
        return

