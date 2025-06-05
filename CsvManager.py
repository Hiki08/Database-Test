from Imports import *
import CsvReader

compiledFrame = None

def CombineDateAndTime():
    global compiledFrame

    emptyColumn = [
        "Process 1 DATA No",
        "Process 1 DATE AND TIME",
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
        "Process 1 Repaired Action"
    ]

    compiledFrame = pd.DataFrame(columns=emptyColumn)

    