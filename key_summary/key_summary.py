from pyspark import SparkConf, SparkContext
import sys

"""
Supports local and distributed files/folders.
"""
def collect_key_total(sc, args):
    print '>>> Reading input file', args[0]
    file = sc.textFile(args[0])

    print '>>> Filtering out blank lines and extracting records...'
    records = file.filter(lambda line: len(line) > 0).map(lambda line: line.strip().split('\t'))

    print '>>> Filtering out invalid records...'
    records = records.filter(lambda f: len(f) == int(args[1]))    

    print '>>> Creating (K, V) with field index:', args[2], ',', args[3]
    key_value = records.map(lambda x: (x[int(args[2])], float(x[int(args[3])])))

    print '>>> Skipping zero value keys'
    #If you need zero values for keys then delete the following line'

    key_value = key_value.filter(lambda (x, y): y > 0)

    return key_value

def collect_summary(key_value, output_file):

    #Finding the min and max for each key
    min_max = key_value.combineByKey(lambda value: [value, value],
                                     lambda x, y: [min(x[0], y), max(x[1], y)],
                                     lambda x, y: [min(x[0], y[0]), max(x[1], y[1])])
    
    #Finding the total and count for each key
    key_value = key_value.combineByKey(lambda value: [value, 1],
                                       lambda x, y: [x[0] + y, x[1] + 1],
                                       lambda x, y: [x[0] + y[0], x[1] + y[1]])

    #Find the average
    key_value = key_value.map(lambda (key, (total, count)): (key, [count, total, total/count]))
    
    #Join the results
    key_value = key_value.join(min_max)
    
    #Merge the values into a single list
    key_value = key_value.map(lambda (key, ((count, total, avg), (_min, _max))): (key, [count, total, avg, _max, _min]))
    
    print '>>> Collecting and saving results at', output_file
    key_value.saveAsTextFile(output_file)

    print '>>> Summary format: [key] [count] [total] [average] [max] [min]'
  
if __name__ == '__main__':

    if len(sys.argv) != 6:
        print '>>> Usage: <Spark submit options> key_summary.py input_file_url no_of_fields key_field_index value_field_index output_folder'
        sys.exit(-1)
          
    sc = SparkContext(conf = SparkConf().setAppName('key_summary'))
    
    collect_summary(collect_key_total(sc, sys.argv[1:]), sys.argv[5])

