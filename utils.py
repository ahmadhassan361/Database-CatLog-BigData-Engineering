import csv
def append_in_file(data,isAppend=True):
    if isAppend:
        with open(r'./output/output.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow(data)
    else:
        with open(r'./output/output.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(data)