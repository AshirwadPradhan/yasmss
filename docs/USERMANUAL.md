## Yet Another SQL to Map-reduce and Spark Segregator (YASMSS)
-----

### USER MANUAL

    This is built and tested on Hadoop 3.1.2 and Spark 2.4.3 which need Java 1.8.0_221 as dependency.
    This is built and tested on Python 3.6 and later.
    This project is only supported on Linux environment until now. (Windows support is on the way)  

>#### Download Links:
>[Hadoop 3.1.2](https://hadoop.apache.org/release/3.1.2.html)    
>[Spark 2.4.3](https://spark.apache.org/downloads.html)   
[Java 1.8.0](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)  
[Python](https://www.python.org/downloads/)

#### USING THIS PROJECT:
After all the pre-requisites mentioned above is installed on the machine then run the following commands on the terminal of your choice.

1. Clone this Repository `git clone https://github.com/AshirwadPradhan/yasmss.git`
2. `cd` into the directory `yasmss/yasmss`
3. Then run `pip3 install -r requirements.txt`
4. Then setup the following config files inside the project:
    >4.1 Open `config.yaml` in your favoraite code editor and modify the following config without the `{}`
    ```yaml
    pathconfig:
        host_ip_port : {insert path here For ex - https://localhost:9000}
        hadoop_streaming_jar: {insert path here}
        input_dir: {insert path here}
        parent_output_dir: {insert path here}
        child_output_dir: {insert path here}
    ```
    >4.2 `cd` into `schema` directory  
    >4.3 Open `schemas.yaml` in your favoraite code editor. This file contains schema of all the tables to be used. Add the desired table schema in the following format: 
    ```yaml
    users:
        userid : IntegerType
        age:  IntegerType
        gender: StringType
        occupation: StringType
        zipcode: StringType 
    ```
5. Now `cd ..` into the parent directory `yasmss`
6. Run `python core.py` and it will deploy the application on the localhost.
7. Go the address prompted by the flask using your browser and submit your query to receive the JSON reponse.