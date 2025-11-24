# DECS Project - HTTP Key-Value Store



---

#  Project Structure

```
project/
 ├── include/
 │    ├── httplib.h
 │    ├── cache.h
 ├── src/
 │    ├── main.cpp         (server)
 │    ├── loadgen.cpp      (load generator)
 ├
 ├── README.md
```

---

#  Compile the Server & Load Generator



This generates:

- `server`  
- `loadgen`

---

#  Running the Server

### **Start MySQL**
```
sudo service mysql start

```

### **Compile Server**
```
g++ main.cpp -o server -I../include -lmysqlclient -lpthread

```



### **Run Server**
```
./server localhost root <password> keyvalueDB
```

Example:
```
./server localhost root Hsrahay@123 keyvalueDB
```

---

# Testing Basic Functionality

### **Set a key**
```
curl -X POST "http://localhost:8080/set" -d "key=name&value=harshay"
```

### **Get a key**
```
curl "http://localhost:8080/get?key=name"
```

### **Delete a key**
```
curl -X DELETE "http://localhost:8080/delete?key=name"
```


---

#  Workloads for Load Generator

Your load generator supports **four workloads**:

| Workload       | Meaning |
|----------------|---------|
| `put_all`      | Only SET operations (DB I/O bound) |
| `get_all`      | Only GET operations with random keys → frequent misses (DB bound) |
| `get_popular`  | Only GET on one hot key → cache hits (memory/CPU bound) |
| `cpu_bound`    | Only compute endpoint → CPU bound |

---

#  Running Load Generator

```
g++ loadgen.cpp -o loadgen -std=c++17 -pthread -lcurl

```

### Format:
```
./loadgen <server_url> <threads> <duration_sec> <workload> <keyspace>
```

---

# **Example Runs**

### **Workload 1: PUT_ALL (I/O bound)**
```
./loadgen http://localhost:8080 2 10 put_all 1000
./loadgen http://localhost:8080 4 10 put_all 1000
./loadgen http://localhost:8080 6 10 put_all 1000
./loadgen http://localhost:8080 8 10 put_all 1000
./loadgen http://localhost:8080 10 10 put_all 1000
```

---

### **Workload 2: GET_ALL (I/O bound)**
```
./loadgen http://localhost:8080 2 10 get_all 1000
./loadgen http://localhost:8080 4 10 get_all 1000
./loadgen http://localhost:8080 6 10 get_all 1000
./loadgen http://localhost:8080 8 10 get_all 1000
./loadgen http://localhost:8080 10 10 get_all 1000
```

---

### **Workload 3: GET_POPULAR (Cache bound)**
```
./loadgen http://localhost:8080 2 10 get_popular 1
./loadgen http://localhost:8080 4 10 get_popular 1
./loadgen http://localhost:8080 6 10 get_popular 1
./loadgen http://localhost:8080 8 10 get_popular 1
./loadgen http://localhost:8080 10 10 get_popular 1
```

---

### **Workload 4: CPU_BOUND**
```
./loadgen http://localhost:8080 2 10 cpu_bound 1
./loadgen http://localhost:8080 4 10 cpu_bound 1
./loadgen http://localhost:8080 6 10 cpu_bound 1
./loadgen http://localhost:8080 8 10 cpu_bound 1
./loadgen http://localhost:8080 10 10 cpu_bound 1
```

---

