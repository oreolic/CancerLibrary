# CancerLibrary
Input for UMI analyzer was generated by Random Barcode Result of "https://github.com/CRISPRJWCHOI/CRISPR_toolkit.git" . You need two different random barcode results which were control and treatment result. then, UMI analyzer will randomly divide all UMI in each sgRNA into number of groups that you set, and calculate RPM(read per million) of every UMI group. Using UMI count matrix, you can perform MAGeCK. At this time, each UMI group was regarded as sgRNA, and each sgRNA was regarded as gene when performing MAGeCK (refer to https://sourceforge.net/p/mageck/wiki/Home/)


require library: pandas, Mageck
you can install MAGeCK package from https://sourceforge.net/p/mageck/wiki/Home/


Example
1. check input files in Input/Random_Barcode_RAW_Data ## Random_Barcode file was generated by "https://github.com/CRISPRJWCHOI/CRISPR_toolkit.git"
2. File Name should be dataset_replicate_cont/treat_all_random_barcode.txt (ex A1_R1_D10_all_random_barcode.txt)
4. type command in terminal 'python UMI_Analyzer.py A1 R1 D10 D24' ##without ' ## this command perform collapse UMI and calcaculate RPM
5. then you can perform MAGeCK. To perform MAGeCK, type command 'python Gx19_Mageck.py A1 R1 D10 D24' then Mageck result will be generated in GX19_Mageck directory
