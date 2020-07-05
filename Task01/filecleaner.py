import pandas as pd
import glob


def correctSubtitleEncoding(filename, newFilename, encoding_from, encoding_to='UTF-8'):
    with open(filename, 'r', encoding=encoding_from) as fr:
        with open(newFilename, 'w', encoding=encoding_to) as fw:
            for line in fr:
                fw.write(line[:-1]+'\r\n')

path = r"/Users/darmora/Desktop/UIUC/CCCapstone/aviationdata/alldata" # use your path
print(path)
all_files = glob.glob(path + "/*.csv")
print(all_files)

li = []

for filename in all_files:
	print(filename)
	correctSubtitleEncoding(filename,filename+"new","ISO-8859-1")
	df = pd.read_csv(filename+"new", index_col=None, header=0)
	df.dropna(subset=['ArrDelay'])
	df = df[['Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate','UniqueCarrier', 'AirlineID','FlightNum','Origin','Dest','DepTime','DepDelay', 'ArrTime', 'ArrDelay']]
	#df.show(10)
	print(df.head())
	li.append(df)



print("Completed computing data frames for each csv")
frame = pd.concat(li, axis=0, ignore_index=True)
print("Concatenated frames and now writing to new file")
frame.to_csv('refined_combined_data.csv.gz', encoding='utf-8',compression='gzip',index=False)

