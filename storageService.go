package main

import (
    "fmt"
    "net/http"
    "io/ioutil"
    "os"
    "net/url"
    "io"
)

func main()  {
    if !registerInKVStore() {
        return
    }

    http.HandleFunc("/sendImage", receiveImage)
    http.HandleFunc("/getImage", serveImage)
    http.ListenAndServe(":3002", nil)
}

func receiveImage(w http.ResponseWriter, r *http.Request)  {
    if r.Method == http.MethodPost {
        values, err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Error 🚫:", err)
            return
        }
        if len(values.Get("id")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input id.")
            return
        }
        // We check values of request from client
        if values.Get("state") != "working" && values.Get("state") != "finished" {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong inpnut state.")
            return
        }

        _, err = strconv.Atoi(values.Get("id"))
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input id.")
            return
        }
        // We create empty file in tmp/state dir with right ID
        file, err := os.Create("/tmp/" + values.Get("state") + "/" + values.Get("id") + ".png")
        defer file.Close()
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        // We copy over image data from request to file
        _, err = io.Copy(file, r.Body)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        fmt.Fprint(w, "success")
    } else {
        w.WriteHeader(http.StatusBadRequest)
        fmt.Fprint(w, "Error 🚫: Only POST accepted.")
    }
}

func serveImage(w http.ResponseWriter, r *http.Request)  {
    if r.Method == http.MethodGet {
        values, err := url.ParseQuery(r.URL.RawQuery)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }
        if len(values.Get("id")) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input id.")
            return
        }
        if values.Get("state") != "working" && values.Get("state") != "finished" {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, "Wrong input state.")
            return
        }

        _, err = strconv.Atoi(values.Get("id"))
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        file, err := os.Open("/tmp/" + values.Get("State") + "/" + values.Get("id") + ".png")
        defer file.Close()
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            fmt.Fprint(w, err)
            return
        }

        _, err = io.Copy(w, file)
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

func registerInKVStore() bool {
    if len(os.Args) < 3 {
        fmt.Println("Error 🚫: Too few arguments.")
        return false
    }
    storageAddress := os.Args[1]
    kVStoreAddress := os.Args[2]

    response, err := http.Post("http://" + kVStoreAddress + "/set?key=storageAddress&value=" + storageAddress, "", nil)
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
