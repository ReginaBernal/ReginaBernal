import pandas as pd
from config import script_config
from utils.dataframe import read_dataframe
from versions import versions
import sys

latest = list(versions.keys())[-1]

req_version = latest

if len(sys.argv) > 1:
    req_version = sys.argv[1]

runners = versions[req_version]


df = [ read_dataframe(path,delimiter=delimiter) for path,delimiter in script_config.INPUT_PATHS ]
# df = [ pd.read_csv(path,delimiter=delimiter) for path,delimiter in script_config.INPUT_PATHS ]
new_df = df.copy()

for step in script_config.STEPS:
    new_df = runners[step](new_df,script_config)

new_df.to_csv(script_config.OUTPUT_PATH,index=False)


