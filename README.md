# tagaddod-data-engineering-task
In this task, I created an ETL pipeline to make the collected data from the tracking tools which records collectors’
locations, device ids, collector ids, and timestamps every two seconds proper for the data analysts to
generate the required map.

## The project strucutre
The project consistes of 3 parts that drives the data into the proper form for further analysis, which are:
- Extraction
- Transformation
- Loading

I used print statements to describe the processes taking during running the code. I tried to keep it simple so the code can run smoothly 
anywhere.
### Extraction
In this part, The collected data was available in four json format files. I extracted the data from these files into 
dictionary.

### Transformation
In this part, I began to explore the data in each file and found that most problems exists in all sources. I applied 
number of transformations, Which are:
- Desolve the structure of json file and transform it to Pandas DataFrame.
- There is a problem where there is a key named mata-data inside the keys of the recorded data.
- Set the collector ID to the collector ID in meta-data since it is a single truth value.
- Set null values to 0 in destination_request_id with consideration that 0 means a null value.
- Drop null values based on latitude and longitude since these columns is very important for the map.
- Drop duplicates base on snapshot_datetime as snapshot_datetime are the most unique colume in the dataset.
- Convert columns data types to the most suitable types.
- Reorder columns to the convenient order. 
- Concate json files into one.

![alt text](https://cdn.discordapp.com/attachments/971742028981501986/1005892370082058270/task.png)

### Loading
In this part, I loaded the new tranformed data into one json file.

## How to run
- Install python 3.x
- In terminal run, ```pip install pandas```
- In terminal run, ```python main.py```
