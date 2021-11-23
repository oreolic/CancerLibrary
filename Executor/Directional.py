#%%
import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime



# %%

class clsDirectional:

    def _Make_Every_ED1_Seq(self,Rnd_Barcode):
        Base = ['A', 'T', 'G', 'C']
        ED1_list = []
        for i in range(len(Rnd_Barcode)):
            if i == 0:
                for N in Base:
                    ED1 = N + Rnd_Barcode[1:]
                    if ED1 == Rnd_Barcode:
                        pass
                    else:
                        ED1_list.append(ED1)
            elif i == len(Rnd_Barcode) - 1:
                for N in Base:
                    ED1 = Rnd_Barcode[:-1] + N
                    if ED1 == Rnd_Barcode:
                        pass
                    else:
                        ED1_list.append(ED1)
            else:
                for N in Base:
                    ED1 = Rnd_Barcode[:i] + N + Rnd_Barcode[i + 1:]
                    if ED1 == Rnd_Barcode:
                        pass
                    else:
                        ED1_list.append(ED1)

        return ED1_list

    def _Possible_ED1(self, df):
        rndList = list(df.iloc[:,1])
        rnd_dic = dict(zip(df.iloc[:,1],df.iloc[:,2]))

        tupList = []
        final = []
        for rnd in rndList:
            if type(rnd) != str:                
                pass
            else:            
                if rnd.find('N') != -1:                    
                    pass
                else:
                    if type(rnd) != str:                        
                        pass
                    else:
                        possibleEd1 = clsDirectional()._Make_Every_ED1_Seq(rnd)
                        trueEd1     = [i for i in possibleEd1 if i in rndList]
                        if len(trueEd1) == 0:
                            final.append((rnd,rnd_dic[rnd]))
                        else:
                            for k in trueEd1:
                                tupList.append([(rnd,rnd_dic[rnd]),(k,rnd_dic[k])])
        
        ## Final: Rnd without ED1 in the total Rnd List
        ## tupList: Rnd tup list with ED1 in the total Rnd List
        return tupList, final

    def Compare_Count(self,A, B):

        if A >= 3 * B - 1:
            return 1
        else:
            return 0

    def _Same_Count(self, Ed1tupList):
        LIST = []
        for tup in Ed1tupList:
            if tup[0][1] == tup[1][1]:
                LIST.extend(tup)
            else:
                pass
        LIST = list(set(LIST))
        return LIST

    def _ED1_Group(self, possibleED1):
        ## possibleED1 = return of _Possible_ED1
        ED1tupList = possibleED1[0]
        final = possibleED1[1] 

        trueEd1 = [ tup for tup in ED1tupList if clsDirectional().Compare_Count(tup[0][1],tup[1][1]) == 1]
        same = self._Same_Count(ED1tupList)
        

        ## True Ed1 Re-Grouping 
        connode_dic = {}
        for tup in trueEd1:
            if tup[1] not in connode_dic:
                connode_dic[tup[1]] = [tup[0]]
            else:
                connode_dic[tup[1]].append(tup[0])

        for con in connode_dic:
            mainList = connode_dic[con]
            if len(mainList) ==1:
                pass
            else:
                mainList.sort(key = lambda x:-x[1]) ##Rnd 많은순
                trueMain = mainList[0]
                connode_dic[con] = [trueMain]
        
        dic = {}
        for tup in connode_dic:
            mainnode =connode_dic[tup][0]
            if mainnode not in dic:
                dic[mainnode] = [tup]
            else:
                dic[mainnode].append(tup)

        for tup in connode_dic:
            if tup not in dic:
                pass
            else:
                mainnode = connode_dic[tup][0]
                dic[mainnode].extend(dic[tup])

        allCons = []
        for tup in dic:
            tupList = dic[tup]
            allCons.extend(tupList)
            if len(tupList) == 1:
                pass
            else:
                for p in tupList:
                    if p not in dic:
                        pass
                    else:
                        dic[tup].extend(dic[p])
            dic[tup] = list(set(dic[tup]))
            
        allCons = list(set(allCons))

        finalDic = {}
        for tup in dic:
            if tup not in allCons:
                finalDic[tup] = dic[tup]
            else:
                pass


        dic = finalDic
        overlap = []
        for t in dic:
            if t not in same:
                pass
            else:
                overlap.append(t)
                
            for k in dic[t]:        
                if k not in same:
                    pass
                else:
                    overlap.append(k)

        truesame = [i for i in same if i not in overlap]

        for tup in truesame:
             final.append(tup)

        
        for tup in dic:
            read = tup[1]+sum([i[1] for i in dic[tup]])
            final.append((tup[0],read))

        df = pd.DataFrame(final)
        return df


    def _Directional_Adjacency(self, eachDF):
        SB = eachDF.iloc[0,0]
        possibleED1 = self._Possible_ED1(eachDF)
        finalRnD   = self._ED1_Group(possibleED1)
        
        finalRnD['Sorting_barcode'] = SB
        finalRnD = finalRnD[['Sorting_barcode',0,1]]
        return finalRnD


    def Multi_Directional_Adjacency(self, Splice_PDS_list):
        t1 = datetime.now()
        with ProcessPoolExecutor(max_workers=25) as executor:
            futs = []

            for eachDF in Splice_PDS_list:
                fut = executor.submit(self._Directional_Adjacency, eachDF)
                futs.append(fut)

            merged = []
            for fut in futs:
                merged.append(fut.result())
            final_pds = pd.concat(merged)
            t2 = datetime.now()
            print('Directional_Adjacency: ', t2-t1)
            return final_pds


# %%
def editDistance(x, y):
    # Create distance matrix
    D = []
    for i in range(len(x)+1):
        D.append([0]*(len(y)+1))
    # Initialize first row and column of matrix
    for i in range(len(x)+1):
        D[i][0] = i
    for i in range(len(y)+1):
        D[0][i] = i
    # Fill in the rest of the matrix
    for i in range(1, len(x)+1):
        for j in range(1, len(y)+1):
            distHor = D[i][j-1] + 1
            distVer = D[i-1][j] + 1
            if x[i-1] == y[j-1]:
                distDiag = D[i-1][j-1]
            else:
                distDiag = D[i-1][j-1] + 1
            D[i][j] = min(distHor, distVer, distDiag)
    # Edit distance is the value in the bottom right corner of the matrix
    return D[-1][-1]
# %%
