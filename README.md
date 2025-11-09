# HTTP BASED KV STORE 

### Features 
- have Database using MySQL
- can have mutliple client because of using httplib library
- In future I implement the the Load generator and Cache for the performance checking


## Compiling code for the Project 

- g++ server.cpp -o server -std=c++17 -lmysqlclient -lpthread


### then for running it 
- ./server



# testing 

## for creating and updating 
- curl "http://localhost:8080/set?key=name&value=harshay"

## for delteing the key value pair
- curl "http://localhost:8080/delete?key=name"

## for get the value from the key
- curl "http://localhost:8080/get?key=name"

this will do in a new terminal 
