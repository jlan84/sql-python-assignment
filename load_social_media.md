# Load Data Into Docker Postgres:    


1. Get data on Docker:    
   - If you have mounted a drive (-v "PWD":/home/data)   
        1) Put data file in folder you launched the `docker run` command (or whatever folder you mounted)    

   - If you did not mount a folder (using `docker copy`)    
        1) Go to folder with file    
        2) Copy the file `docker cp <file_name> <docker_container_name>:<path_in_docker_container>`    
         example `docker cp socialmedia.sql pgserv:/home/data`    

     - Note: You can not do a docker copy into a mounted drive.   

2. Get a bash shell in your docker container   
    - `docker exec -it <container_name> bash`   

3. Navigate to location of the file   

4. Log into postgres   
    - `psql -U postgres`  
    - Note: we are specifying signing in as user postgres   

5. Create the socialmedia database   
    - `CREATE DATABASE socialmedia;`     

6. Quit postgres    

7. Load the data into Postgres  
    - `psql -U postgres socialmedia < socialmedia.sql`   
