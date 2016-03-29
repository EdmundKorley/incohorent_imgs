package main

import (
    "net/http"
    "net/url"
    "fmt"
    "sync"
    "time"
    "strconv"
    "encoding/json"
    "os"
    "io/ioutil"
)

// A Task data-type that we will use for storing tasks
type Task struct {
    ID int `json:"id"`
    State int `json:"state"`
}

var dataStore []Task
var dataStoreMutex sync.RWMutex
var oldestNotFinishedTask int
var oNFTMutex sync.RWMutex

func main() {

    if !registerInKVStore() {
        return
    }

    dataStore = make([]Task, 0)
    dataStoreMutex = sync.RWMutex{}
    oldestNotFinishedTask = 0
    oNFTMutex = sync.RWMutex{}

    http.HandleFunc("/getByID", getByID)
    http.HandleFunc("/newTask", newTask)
    http.HandleFunc("/getNewTask", getNewTask)
    http.HandleFunc("/finishTask", finishTask)
    http.HandleFunc("/setByID", setByID)
    http.HandleFunc("/list", list)
    fmt.Println("taskService is up! 📫")
    http.ListenAndServe(":3001", nil)
}

func getByID(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodGet {
        values, err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            fmt.Fprint(w, err)
            return
        }
        if len(values.Get("id")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input")
            return
        }

        // Turn parsed ID to integer
        id, err := strconv.Atoi(string(values.Get("id")))

        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        dataStoreMutex.RLock()
        // Reading length of store (to check for task not yet added) must be done synchronously
        // hence the use of the mutex
        bIsInError := err != nil || id >= len(dataStore)
        dataStoreMutex.RUnlock()

        if bIsInError {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input")
            return
        }

        dataStoreMutex.RLock()
        value := dataStore[id]
        dataStoreMutex.RUnlock()

        response, err := json.Marshal(value)

        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        fmt.Fprint(w, string(response))
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only GET accepted.")
    }
}

func newTask(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodPost {
        dataStoreMutex.Lock()

        // Create new Task with next ID and add it to our dataStore
        taskToAdd := Task{
            ID: len(dataStore),
            State: 0,
        }
        dataStore[taskToAdd.ID] = taskToAdd
        dataStoreMutex.Unlock()

        // Return task ID to client
        fmt.Fprint(w, taskToAdd.ID, "added successfully 😜")
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only POST accepted")
    }
}

func getNewTask(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodPost {
        bErrored := false
        dataStoreMutex.RLock()
        if len(dataStore) == 0 {
            bErrored = true
        }
        dataStoreMutex.RUnlock()

        if bErrored {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error 🚫: No non-started task.")
            return
        }

        taskToSend := Task{ ID: -1, State: 0 }

        oNFTMutex.Lock()
        dataStoreMutex.Lock()
        // Find oldest task that hasn't started yet
        for i := oldestNotFinishedTask; i < len(dataStore); i++ {
            if dataStore[i].State == 2 && i == oldestNotFinishedTask {
                oldestNotFinishedTask++
                continue
            }
            if dataStore[i].State == 0 {
                dataStore[i] = Task{ ID: i, State: 1 }
                taskToSend = dataStore[i]
                break
            }
        }
        dataStoreMutex.Unlock()
        oNFTMutex.Unlock()

        if taskToSend.ID == -1 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error 🚫: No non-started task.")
            return
        }

        myID := taskToSend.ID

        // Start a goroutine that will change start back to not started
        // if still in progress after 120 seconds
        go func() {
            time.Sleep(time.Second * 120)
            dataStoreMutex.Lock()
            if dataStore[myID].State == 1 {
                dataStore[myID] = Task{ ID: myID, State: 0 }
            }
        }()

        response, err := json.Marshal(taskToSend)

        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        fmt.Fprint(w, string(response))
    }
}

func finishTask(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodPost {
        values, err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            fmt.Fprint(w, err)
            return
        }
        if len(values.Get("id")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input")
            return
        }

        id, err := strconv.Atoi(string(values.Get("id")))
        if err != nil {
            fmt.Fprint(w, err)
            return
        }

        updatedTask := Task{ ID: id, State: 2 }
        bErrored := false

        dataStoreMutex.Lock()
        if dataStore[id].State == 1 {
            dataStore[id] = updatedTask
        } else {
            bErrored = true
        }
        dataStoreMutex.Unlock()

        if bErrored {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input")
            return
        }

        fmt.Fprint(w, "success")
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only POST accepted")
    }
}

func setByID(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodGet {
        taskToSet := Task{}
        data, err := ioutil.ReadAll(r.Body)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        // Attempt to unmarshal the request (if it succeeds we place into our taskToSet)
        err = json.Unmarshal([]byte(data), &taskToSet)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        bErrored := false
        dataStoreMutex.Lock()
        if taskToSet.ID >= len(dataStore) || taskToSet.State > 2 || taskToSet.State < 0 {
            bErrored = true
        } else {
            dataStore[taskToSet.ID] = taskToSet
        }
        dataStoreMutex.Unlock()

        if bErrored {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error 🚫: Wrong input")
        }

        fmt.Fprint(w, "success")
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only POST accepted")
    }
}

func list(w http.ResponseWriter, r *http.Request) {
    if r.Method == http.MethodGet {
        dataStoreMutex.RLock()
        for key, value := range dataStore {
            fmt.Fprintln(w, "KEY:", key, "ID:", value.ID, "STATE:", value.State)
        }
        dataStoreMutex.RUnlock()
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only GET accepted.")
    }
}

func registerInKVStore() bool {
    if len(os.Args) < 3 {
        fmt.Println("Error 🚫: Too few arguments.")
        return false
    }
    databaseAddress := os.Args[1] // Address of executable itself
    kVStoreAddress := os.Args[2] // Address of kVService.go

    response, err := http.Post("http://" + kVStoreAddress + "/set?key=databaseAddress&value=" + databaseAddress, "", nil)
    if err != nil {
        fmt.Println(err)
        return false
    }

    data, err := ioutil.ReadAll(response.Body)

    if err != nil {
        fmt.Println(err)
        return false
    }
    if response.StatusCode != http.StatusOK {
        fmt.Println("Error: Failure when contacting key-value store: ", string(data))
        return false
    }
    return true

}
