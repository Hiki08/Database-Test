from Imports import *

dfVt1 = None

def ReadProcess1Csv():
    global dfVt1
    
    vt1Directory = (r'\\192.168.2.19\ai_team\AI Program\Outputs\FC1 CSV\VT1')
    os.chdir(vt1Directory)
    dfVt1 = pd.read_csv('log000_1.csv', encoding='latin1')
    dfVt1.columns = ["Process 1 DATA No",
        "Process 1 DATE",
        "Process 1 TIME",
        "Process 1 Model Code",
        "Process 1 S/N",
        "Process 1 ID",
        "Process 1 NAME",
        "Process 1 Regular/Contractual",
        "Process 1 Em2p",
        "Process 1 Em2p Lot No",
        "Process 1 Em3p",
        "Process 1 Em3p Lot No",
        "Process 1 Harness",
        "Process 1 Harness Lot No",
        "Process 1 Frame",
        "Process 1 Frame Lot No",
        "Process 1 Bushing",
        "Process 1 Bushing Lot No",
        "Process 1 ST",
        "Process 1 Actual Time",
        "Process 1 NG Cause",
        "Process 1 Repaired Action"]
    
    # Merge date and time columns into a single datetime column
    dfVt1['Process 1 DateTime'] = pd.to_datetime(dfVt1['Process 1 DATE'] + ' ' + dfVt1['Process 1 TIME'])
    
    # Optionally drop the original date and time columns if you don't need them anymore
    dfVt1 = dfVt1.drop(['Process 1 DATE', 'Process 1 TIME'], axis=1)
    
    # Reorder columns to move DateTime to second position
    cols = dfVt1.columns.tolist()
    cols.remove('Process 1 DateTime')
    cols.insert(1, 'Process 1 DateTime')
    dfVt1 = dfVt1[cols]