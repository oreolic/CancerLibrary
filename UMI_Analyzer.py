#%%
from Executor import Preprocessing as PPC
from Executor import Mageck_Converter as MG
from Executor import RPM
from Executor import DataParsing as DP
import pandas as pd
import sys

#%%
def main(Data_combination):
    PPC.DataPreProcessing(Data_combination)
    RPM.TotalRPM().Return_Bread(Data_combination)        
    return 

if __name__ == '__main__': 
    data_combination = [(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])]
    main(data_combination)


