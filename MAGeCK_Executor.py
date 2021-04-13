import pandas as pd
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from Executor import dataparsing as dp
import os
from time import sleep


def new_mageck_input(eachdf):
    sb = eachdf.iloc[0,0]
    eachdf = eachdf.set_index('D10_Count')

    lst = []
    for idx in list(set(eachdf.index)):
        d10 = eachdf.loc[idx]
        if str(type(d10)) == "<class 'pandas.core.frame.DataFrame'>":
            d10rpm = d10['D10_RPM'].median()
            d24rpm = d10['D24_RPM'].median()
        else:
            d10rpm = d10['D10_RPM']
            d24rpm = d10['D24_RPM']
        
        eachlist = [sb,d10rpm,d24rpm]
        lst.append(eachlist)

    
    eachdf = pd.DataFrame(lst, columns = ['Sorting_Barcode','D10_RPM','D24_RPM'])
    return eachdf




def median_rpm(mag_lst,umi_cutoff):
    with ProcessPoolExecutor(max_workers=25) as executor:
        futs = []

        for eachdf in mag_lst:
            if eachdf.shape[0] <=umi_cutoff:
                pass
            else:
                fut = executor.submit(new_mageck_input,eachdf)
                futs.append(fut)

        merged = []

        for fut in futs:
            merged.append(fut.result())
            
    final = pd.concat(merged)
    return final




def mageck_executor(data_combination,umi_cutoff,co1=5):
    tup = data_combination[0]
    d10 = tup[2]
    d24 = tup[3]


    mag = pd.read_csv('Result/{}_{}_totalRPM_FC_Result.txt'.format(data_combination[0][0], data_combination[0][1]), sep='\t')
    mag.columns = ['Sorting_Barcode', 'RandomBarcode', '{}_Count'.format('D10'), '{}_Count'.format('D24')
                                                 ,'{}_RPM'.format('D10'),'{}_RPM'.format('D24'), 'Fold_Change']
    mag = mag[mag['D10_Count']>=co1]

    mag_lst = dp.data_parsing().divide_df_into_list(mag)
    final = median_rpm(mag_lst,umi_cutoff)

    rnd = ['RND{}'.format(int(i)) for i in range(final.shape[0])]
    final['RandomBarcode'] = rnd
    final = final[['RandomBarcode','Sorting_Barcode','{}_RPM'.format('D10'),'{}_RPM'.format('D24')]]
    final.columns = [['RandomBarcode','Sorting_Barcode','{}_RPM'.format(d10),'{}_RPM'.format(d24)]]
    final.to_csv('MedianRPM_Mageck/{}_{}_MedianRPM_Mageck_UMI_over_{}_Input.txt'.format(data_combination[0][0], data_combination[0][1],co1),sep='\t',index=None)
 
    return final






def main(i,co1,umi_cutoff):

    print('Start {}_{}'.format(i[0],i[1]))
    mageck_executor([i],umi_cutoff,co1)
    print('Make_Input file done')        
    os.system('mageck test -k MedianRPM_Mageck/{dataset}_{rep}_MedianRPM_Mageck_UMI_over_{co}_Input.txt'.format(dataset = i[0],rep = i[1],co=co1,umi_cutoff =umi_cutoff )+
    ' -c 0 -t 1 --norm-method median'+' -n MedianRPM_Mageck/'+
    '{dataset}_{rep}_MedianRPM_D10_over_{co}_UMI_over_{umi_cutoff}_Mageck'.format(dataset = i[0],rep = i[1],co=co1,umi_cutoff =umi_cutoff ))

    return

####
#

data_combination = [('2KABE','R1','D10','D24')]
co1 = 5
umi_cutoff = 0
##

for i in data_combination:
    print(i)
    main(i,co1,umi_cutoff)



import os
res_lst = os.listdir('MedianRPM_Mageck')
for i in res_lst:
    if i.find('gene_summary') != -1:
        pass
    elif i.find('Input') != -1:
        pass
    else:
        os.remove('{}/{}'.format('MedianRPM_Mageck',i))