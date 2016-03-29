package main

import (
    "os"
    "fmt"
    "net/http"
    "io/ioutil"
    "io"
    "encoding/json"
    "net/url"
)

type Task struct {
    ID int `json:"id"`
    State int `json:"state"`
}

var databaseLocation string
var storageLocation string

func main()  {
    // Register in database
    if !registerInKVStore() {
        return
    }

    // A redundant getting of storageLocation and databaseLocation
    // (after getting them in registerInKVStore)
    // This is to allow access to these addresses via lexical scope from our route handlers
    kVStoreAddress := os.Args[2]

    response, err := http.Get("http://" + kVStoreAddress + "/get?key=databaseAddress")
    if response.StatusCode != http.StatusOK {
        fmt.Println("Error 🚫: Can't get database address.")
        fmt.Println(response.Body)
        return
    }

    data, err := ioutil.ReadAll(response.Body)
    if err != nil {
        fmt.Println(err)
        return
    }
    databaseLocation = string(data)

    response, err = http.Get("http://" + kVStoreAddress + "/get?key=storageAddress")
    if response.StatusCode != http.StatusOK {
        fmt.Println("Error 🚫: Can't get storage address.")
        fmt.Println(response.Body)
        return
    }

    data, err = ioutil.ReadAll(response.Body)
    if err != nil {
        fmt.Println(err)
        return
    }
    storageLocation = string(data)


    // These route handlers close over the databaseLocation and storageLocation addresses
    // for those microservices
    http.HandleFunc("/new", newImage)
    http.HandleFunc("/get", getImage)
    http.HandleFunc("/isReady", isReady)
    http.HandleFunc("/getNewTask", getNewTask)
    http.HandleFunc("/registerTaskFinished", registerTaskFinished)
    fmt.Println("masterService is up! 😜")
    http.ListenAndServe(":3003", nil)
}

func newImage(w http.ResponseWriter, r *http.Request)  {
    if r.Method == http.MethodPost {
        response, err := http.Post("http://" + databaseLocation + "/newTask", "text/plain", nil)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        id, err := ioutil.ReadAll(response.Body)
        if err != nil {
            fmt.Println(err)
            return
        }

        // Make call to storage microservice with image data
        // Which saves a temp copy of the image file as .png
        _, err = http.Post("http://" + storageLocation + "/sendImage?id=" + string(id) + "&state=working", "image", r.Body)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }
        fmt.Fprint(w, string(id))
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only POST accepted.")
    }
}

func getImage(w http.ResponseWriter, r *http.Request)  {
    if r.Method == http.MethodGet {
        values, err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            fmt.Fprint(w, err)
            return
        }
        if len(values.Get("id")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input.")
            return
        }

        response, err := http.Get("http://" + storageLocation + "/getImage?id=" + values.Get("id") + "&state=finished")
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        // Copy over response to client
        _, err = io.Copy(w, response.Body)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only GET accepted.")
    }
}

func isReady(w http.ResponseWriter, r *http.Request)  {
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

        // Verify all the parameterss and request method
        // Asking databse for the Task requested
        response, err := http.Get("http://" + databaseLocation + "/getByID?id=" + values.Get("id"))
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }
        data, err := ioutil.ReadAll(response.Body)
        if err != nil {
            fmt.Println(err)
            return
        }

        // Parse task and respond to client
        myTask := Task{}
        json.Unmarshal(data, &myTask)

        if (myTask.State == 2) {
            fmt.Fprint(w, "1")
        } else {
            fmt.Fprint(w, "0")
        }
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only POST accepted.")
    }
}

// Part of worker interface
func getNewTask(w http.ResponseWriter, r *http.Request)  {
    if r.Method == http.MethodPost {
        response, err := http.Post("http://" + databaseLocation + "/getNewTask", "text/plain", nil)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        // Copy task over to client
        _, err = io.Copy(w, response.Body)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error: Only POST accepted")
    }
}

// Part of worker interface
// (worker talks to masterService rather than database directly)
func registerTaskFinished(w http.ResponseWriter, r *http.Request)  {
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

        // Register task as finished in database here
        response, err := http.Post("http://" + databaseLocation + "/finishTask?id=" + values.Get("id"), "test/plain", nil)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error:", err)
            return
        }

        _, err = io.Copy(w, response.Body)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error:", err)
            return
        }
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only POST accepted.")
    }
}

func registerInKVStore() bool {
    if len(os.Args) < 3 {
        fmt.Println("Error 🚫: Too few arguments.")
        return false
    }
    masterAddress := os.Args[1]
    kVStoreAddress := os.Args[2]

    response, err := http.Post("http://" + kVStoreAddress + "/set?key=masterAddress&value=" + masterAddress, "", nil)
    if err != nil {
        fmt.Println(err)
        return false
    }
    data, err := ioutil.ReadAll(response.Body)
    if err != nil {
        fmt.Println("Error 🚫: Failure when contacting key-value store:", string(data))
        return false
    }
    return true
}
