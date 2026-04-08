import pandas as pd

#adapted from https://medium.com/parsing-bulk-whois-data/parsing-ripe-bulk-whois-data-8495d5fb5fe9
#By Thomas Gorman
ripe = pd.read_table("ripe.db", encoding="ISO-8859-1")
ripe.columns = ["data"]
ripe = ripe[ripe.data.str.contains("remarks") == False]
ripe = ripe[ripe.data.str.contains("%") == False]
inetnum_loc = ripe.data.str.contains("inetnum").idxmax()
ripe = ripe[inetnum_loc:]
ripe = ripe.reset_index(drop=True)

ripe = ripe["data"].str.split(pat = ":",n = 1, expand = True)
ripe = ripe.set_index([0, (ripe[0] == "inetnum").cumsum().rename("row")])
ripe = ripe.set_index(ripe.groupby([0, "row"]).cumcount(), append = True)
ripe = ripe.reset_index("row")
ripe.index = ripe.index.map("{0[0]}_{0[1]}".format)
print(ripe.head())
ripe = ripe.set_index(["row"], append = True)[1].unstack(level = 0)
ripe = ripe.rename(columns = lambda x: x.split("_0")[0]).reset_index()
print(ripe.head())