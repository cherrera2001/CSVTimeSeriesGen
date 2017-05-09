### CSV Time Series Generator

This is a utility based on [Tsimulus](https://tsimulus.readthedocs.io/en/latest/get_started.html#get-started) that will generate realistic time series data for an arbitrary number of tags and
output those values to a CSV file.

### Clone the repository to your local machine
 ``` sh
> git clone https://github.com/cherrera2001/CSVTimeSeriesGen.git Simulator
 ```
### Navigate to the new directory
  ``` sh
> cd Simulator 
  ```
   
 ### Perform a maven clean install from the root of the codebase
 ``` sh
> mvn clean install
 ```
 
 ### An uber jar should be created in the /target directory
 ``` sh
> cd target
 ```
 
 ### Execute the jar with the following command
 ``` sh
> java -jar uber-tsGeneratedCsv-1.0-SNAPSHOT.jar ../basicConfig.json 
 ```
 
 This will generate a CSV file with all the defaults for 1 hour of data. You can modify the generation configuration by 
 modifying the basicConfig.json file according to the docs at [Tsimulus](https://tsimulus.readthedocs.io/en/latest/get_started.html#get-started)
 
### View the data
 ``` sh
> nano output-0.csv
 ```
 
 ### To see all the options run the following command
 ``` sh
> java -jar uber-tsGeneratedCsv-1.0-SNAPSHOT.jar
 ```