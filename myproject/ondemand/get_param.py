def getVarFromFile(filename):
    import importlib
    f = open(filename)
    global data
    data = importlib.load_source('data', '', f)
    f.close()

# path to "config" file
# C:/Users/SVS0RMR/Downloads/AssetInventory/ondemand/parameters.txt
getVarFromFile('C:/Users/SVS0RMR/Downloads/AssetInventory/ondemand/param.txt')
print (data.var1)
print (data.var2)